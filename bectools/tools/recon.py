from bectools import connectors as con
from collections import namedtuple
import tempfile
from pathlib import Path
import bectools as bt


def get_workflow_runs_ids(runs):
    run_ids = set(runs["WORKFLOW_RUN_ID"])
    return (run_ids, runs)


def compare_session_logs(left_env, right_env, wf_name_left, wf_name_right, task_name, output_dir, last_days=1, start_time_from=None, start_time_end=None, multiprocess=False):
    import os
    left_runs = con.BEC.monitoring.search_workflow_task_runs(left_env, wf_name_left, task_name,
                                                 last_days, start_time_from, start_time_end)
    right_runs =  con.BEC.monitoring.search_workflow_task_runs(right_env, wf_name_right, task_name,
                                                last_days, start_time_from, start_time_end)
    left_logs = get_workflow_logs(left_runs.runs, use_converted_files=True,
                                         output_dir=os.path.join(output_dir,left_env), multiprocess=multiprocess)
    right_logs = get_workflow_logs(right_runs.runs, use_converted_files=True,
                                   output_dir=os.path.join(output_dir, right_env), multiprocess=multiprocess)

    left_logs_analysis = analyze_session_logs(left_logs)
    right_logs_analysis = analyze_session_logs(right_logs)

    logs_comp = compare_log_analysis(left_logs_analysis, right_logs_analysis, left_env, right_env)
    save_log_analysis_comp_to_excel(logs_comp, os.path.join(output_dir, left_env+"_"+right_env+".xlsx"))
    return logs_comp

def analyze_session_logs(logs):
    # analyze parameters

    print("Analyzying logs")
    print("Loading logs to spark.")
    if logs.logs is None or len(logs.logs.index)==0:
        print("Logs dataframe is empty or none given.")
        return
    runs_spark_df = con.SPARK.pandas_to_spark(logs.runs)
    logs_spark_df = con.SPARK.pandas_to_spark(logs.logs)
    print("Loaded.")
    logs_spark_df.registerTempTable("logs_spark_df")
    runs_spark_df.registerTempTable("runs_spark_df")

    def wf_name_basis(wf_name):
        import re
        return re.sub("(_[BK])?_[0-9]+$", "", wf_name)

    con.SPARK.get_spark_session().udf.register("wf_name_basis", wf_name_basis)

    runs_spark_df=con.SPARK.sql("""select 
                    r.server_name, r.subject_area, r.workflow_run_id, r.workflow_name, wf_name_basis(r.workflow_name) workflow_name_basis, r.task_name, r.bank_korsel_nr,
                      (select min(rr.start_time) from runs_spark_df rr where
                       rr.workflow_run_id = r.workflow_run_id) workflow_start_time ,
                     r.start_time, r.log_file,
                      IF((select max(log_workflow_run_id) from logs_spark_df l where
                      r.log_file = l.log_file and r.workflow_run_id = l.log_workflow_run_id) is not null, 'Y','N')  logs_found
                     ,
                      (select max(bankdato_date_param) from logs_spark_df l where
                      r.log_file = l.log_file and r.workflow_run_id = l.log_workflow_run_id) bankdato_date_param 
                 from runs_spark_df r  """)
    runs_spark_df.registerTempTable("runs_spark_df")

    params_df=con.SPARK.sql("""select 
                    r.server_name, r.subject_area, r.workflow_run_id, r.bankdato_date_param, r.workflow_name,r.workflow_name_basis, r.task_name, r.bank_korsel_nr, r.workflow_start_time,
                     timestamp, param_name, param_value 
                 from runs_spark_df r left outer join logs_spark_df p on (r.log_file = p.log_file and r.workflow_run_id = p.log_workflow_run_id) where  param_name is not null """)

    src_trg_df=con.SPARK.sql("""select 
                    r.server_name, r.subject_area, r.workflow_run_id, r.bankdato_date_param, r.workflow_name, r.workflow_name_basis,  r.task_name, r.bank_korsel_nr, r.workflow_start_time,  x.timestamp,
                    x.src_trg_type,
                    x.src_trg_table,
                    x.src_trg_instance,
                    x.src_trg_output_rows,
                    --x.src_trg_affected_rows,
                    --x.src_trg_applied_rows,
                    x.src_trg_rejected_rows,
                    --x.threadName
                    sq_instance.sq_sql
                 from 
                     runs_spark_df r left outer join logs_spark_df x 
                     on (r.log_file = x.log_file  and r.workflow_run_id = x.log_workflow_run_id) 
                      left outer join logs_spark_df sq_instance
                      on  (sq_instance.sq_instance = x.src_trg_instance and
                          sq_instance.log_file = x.log_file  and r.workflow_run_id = sq_instance.log_workflow_run_id
                      )
                      where x.src_trg_table is not null""")


    lkps_df=con.SPARK.sql("""select 
                    r.server_name, r.subject_area, r.workflow_run_id, r.bankdato_date_param, r.workflow_name, r.workflow_name_basis, r.task_name, r.bank_korsel_nr, r.workflow_start_time, x.timestamp,
                    x.lookup_trans,  x.lookup_trans_sql_default_override, lkp_cnt.lookup_rows_count,x.lookup_trans_sql
                 from 
                     runs_spark_df r left outer join logs_spark_df x 
                     on (r.log_file = x.log_file  and r.workflow_run_id = x.log_workflow_run_id) 
                      left outer join logs_spark_df lkp_cnt
                      on  (
                          lkp_cnt.log_file = x.log_file and lkp_cnt.lookup_rows_count is not null
                          and lkp_cnt.threadName like x.threadName||':%'  
                          and r.workflow_run_id = lkp_cnt.log_workflow_run_id
                      )
                      where x.lookup_trans is not null""")

    print("Done.")
    LogAnalysis=namedtuple("LogAnalysis","runs params sources_targets lookups")
    return LogAnalysis(runs_spark_df.toPandas(), params_df.toPandas(), src_trg_df.toPandas(), lkps_df.toPandas())


def compare_log_analysis(log_analysis_left, log_analysis_right, left_name, right_name ):
    # params
    print("Comparing logs.")
    params_keys = ['workflow_name_basis','bank_korsel_nr', 'bankdato_date_param', 'task_name', 'param_name']
    params_comp=bt.init.compare_data_frames(log_analysis_left.params, log_analysis_right.params, params_keys, left_name, right_name ,  ["param_value"])


    src_tgt_keys = ['workflow_name_basis', 'bank_korsel_nr','bankdato_date_param', 'task_name', 'src_trg_instance']
    src_tgt_comp=bt.init.compare_data_frames(log_analysis_left.sources_targets, log_analysis_right.sources_targets, src_tgt_keys, left_name, right_name ,
                                     ["src_trg_output_rows","src_trg_rejected_rows" ])


    lookups_keys = ['workflow_name_basis', 'bank_korsel_nr', 'bankdato_date_param', 'task_name', 'lookup_trans']
    lookups_comp=bt.init.compare_data_frames(log_analysis_left.lookups, log_analysis_right.lookups, lookups_keys, left_name, right_name ,  ["lookup_rows_count"])

    LogAnalysisComp = namedtuple("LogAnalysisComp", "left_runs right_runs  params_comp src_trg_comp lookups_comp left_name right_name")
    return LogAnalysisComp(log_analysis_left.runs, log_analysis_right.runs, params_comp, src_tgt_comp,  lookups_comp, left_name,  right_name)


def save_log_analysis_to_excel(log_analysis, output_file):
    bt.pandas_helper.save_dataframes_to_excel(log_analysis, ["Runs","Params","Sources_Targets","Lookups"], output_file)

def save_log_analysis_comp_to_excel(log_analysis_comp, output_file):
    left_name = log_analysis_comp.left_name
    right_name = log_analysis_comp.right_name
    bt.pandas_tools.save_dataframes_to_excel(log_analysis_comp,
                                              [left_name+ " Runs" ,right_name + " Runs",
                                               left_name +" vs "+right_name + " Params",
                                               left_name +" vs "+right_name + " Src_Trg",
                                               left_name  +" vs "+right_name + " Lookups"], output_file)

def get_workflow_log(log_file,output_dir, use_converted_files, overwrite_converted_files):
    # banknr = r.BANKNR
    # log_file = r.LOG_FILE
    # workflow_name = r.WORKFLOW_NAME
    # workflow_run_id= r.WORKFLOW_RUN_ID
    # task_name=r.TASK_NAME
    # run_status_code=r.RUN_STATUS_CODE
    # server_name=r.SERVER_NAME
    # subject_area=r.SUBJECT_AREA
    import os
    file_name = os.path.basename(log_file)
    output_file = os.path.join(output_dir, file_name + ".xml")
    print(output_file)
    log_df = con.INFA_CMD.FTST2.convert_log_bin_to_xml_and_parse(log_file, output_file=output_file,
                                                                 use_converted_files=use_converted_files,
                                                                 overwrite_converted_files=overwrite_converted_files,
                                                                 output_only_matched=True)[1]

    def add_columns(cols):
        for i, c in enumerate(cols):
            log_df.insert(i, c[0], c[1])
            log_df[c[0]] = c[1]

    add_columns(
        [
            ("log_file", log_file)
            # ("workflow_name", workflow_name),
            # ("banknr", banknr),
            # ("workflow_run_id", workflow_run_id),
            # ("task_name", task_name),
            # ("run_status_code", run_status_code),
            # ("server_name", server_name),
            # ("subject_area", subject_area)
        ]
    )
    return log_df

def get_workflow_logs(runs, use_converted_files=True, overwrite_converted_files=False, output_dir = r"C:\out\temp", multiprocess=False):

    import pandas
    import multiprocessing
    res = None

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # get log name and remove Nones if exist
    logs_to_process_params=[(l,output_dir, use_converted_files, overwrite_converted_files)  for l in set(map(lambda x: x.LOG_FILE, runs.itertuples())) if l]

    if not multiprocess:
        for log_file in logs_to_process_params:
            log_df= get_workflow_log(*log_file)
            if res is None:
                res = log_df
            else:
                #res = tuple( pandas.concat([res, df])  for res, df in zip(res,(log_df,) ) )
                res = pandas.concat([res, log_df])
    else:
        print("Multiprocessing is on.")
        pool = multiprocessing.Pool(8)
        res=pandas.concat(pool.starmap(get_workflow_log, logs_to_process_params))

    Logs=namedtuple("Logs","runs logs")
    return Logs(runs, res)



def old_ms_compare_table_counts(table_df, src_db_con, tested_db_con, banks):

    ftst2_res_dfs = []
    prod_res_dfs = []
    for r in table_df.itertuples():
        table_name = r.TABLE_NAME.replace("dbo.", "")
        table_db_layer = r.DB_LAYER

        sql_cnt_ftst2_stmts = []
        sql_cnt_prod_stmts = []

        for bnk in banks:
            if table_db_layer == 'MDW':
                x = get_tables_meta(src_db_con, tested_db_con, table_name, bnk, "MDW_PROD", "NED_UTST")
                sql_cnt_ftst2_stmts.append(
                    f"""select '{bnk}' as BANK_NR, '{x.table_name}' as TABLE_NAME, '{x.tested_db}' as DB_NAME, count(*) CNT from {x.tested_db}.dbo.{x.table_name}
                    """)

                sql_cnt_prod_stmts.append(
                    f"""select '{bnk}' as BANK_NR, '{x.table_name}' as TABLE_NAME, '{x.src_db}' as DB_NAME, count(*) CNT from {x.src_db}.dbo.{x.table_name}
                    """)

        if len(sql_cnt_ftst2_stmts) > 0:
            sql_cnt_ftst2_sql = "union all ".join(sql_cnt_ftst2_stmts)
            print(sql_cnt_ftst2_sql)
            ftst2_res_dfs.append(tested_db_con.query_pandas(sql_cnt_ftst2_sql))
        if len(sql_cnt_prod_stmts) > 0:
            sql_cnt_prod_sql = "union all ".join(sql_cnt_prod_stmts)
            print(sql_cnt_prod_sql)
            prod_res_dfs.append(src_db_con.query_pandas(sql_cnt_prod_sql))

    import pandas

    res_ftst = pandas.concat(ftst2_res_dfs)
    res_prod = pandas.concat(prod_res_dfs)
    res_ftst = res_ftst.add_suffix("_RECONED")
    res_prod = res_prod.add_suffix("_SRC")

    cross_init_comp = res_ftst.merge(res_prod, left_on=["TABLE_NAME_RECONED", "BANK_NR_RECONED"],
                                     right_on=["TABLE_NAME_SRC", "BANK_NR_SRC"], how="outer")
    return cross_init_comp


def recon_compare_tables(left_db_connector, left_table_name,  right_db_connector, right_table_name, keys, left_workflow_run_ids, right_workflow_run_ids):
    print("comparing tables:" + left_table_name+ " "+right_table_name + " keys:"+str(keys) )
    left_workflow_run_ids_csv = ",".join([str(x) for x in left_workflow_run_ids])
    right_workflow_run_ids_csv = ",".join([str(x) for x in right_workflow_run_ids])

    left_df = left_db_connector.query_spark(f"""select * from {left_table_name}
        where opret_wf_run_i in ({left_workflow_run_ids_csv}) or ret_wf_run_i in ({left_workflow_run_ids_csv})""")

    right_df = right_db_connector.query_spark(f"""select * from {right_table_name}
        where opret_wf_run_i in ({right_workflow_run_ids_csv}) or ret_wf_run_i in ({right_workflow_run_ids_csv})""")

    return con.SPARK.spark_tools.compare_dataframes(left_df, right_df,
                                        key_fields=keys,
                                        exclude_columns=[], mode=1)


def compare_tables_for_banks(left_db_connector, right_db_connector,
                             left_database_pattern, right_database_pattern,
                             table_name, banks, left_wf_run_id, right_wf_run_ids):

    for bnk in banks:
        left_workflow_run_ids_csv = ",".join([str(x) for x in left_wf_run_id])
        right_workflow_run_ids_csv = ",".join([str(x) for x in right_wf_run_ids])

        left_df = left_db_connector.query_pandas(f"""select * from {table_name}
            where opret_wf_run_i in ({left_workflow_run_ids_csv}) or ret_wf_run_i in ({left_workflow_run_ids_csv});""")

        right_df = right_db_connector.query_pandas(f"""select * from {table_name}
            where opret_wf_run_i in ({right_workflow_run_ids_csv}) or ret_wf_run_i in ({right_workflow_run_ids_csv});""")



def recon_workflow(workflow_name, src_db_con, tested_db_con,src_db_suffix, tested_db_suffix,  banks, table_keys):
    trg_tables,_ = get_target_tables(workflow_name)
    tested_workflow_run_ids,_ = get_workflow_last_run_ids(con.DB.UTST.SQL.PC_REP.DS.D00000TD10_PWC_REP_FTST2, workflow_name)
    src_workflow_run_ids,_ = get_workflow_last_run_ids(con.DB.PROD.SQL.PC_REP.DS.D00000PD10_PWC_REP_PROD, workflow_name)

    if len(tested_workflow_run_ids) ==0:
        raise  Exception("Cannot identify test workflow runs")
    if len(src_workflow_run_ids) == 0:
        raise Exception("Cannot identify source workflow runs")

    result=[]
    for tb in trg_tables:
        for bnk in banks:
            db_tables=get_tables_meta(src_db_con, tested_db_con, tb, src_db_suffix, tested_db_suffix)
            keys=[]
            if tb not in table_keys:
                print(tb+ " not in table_keys dict" )
            else:
                keys=table_keys[tb]
            result.append( ( (tb, bnk,db_tables.src_db, db_tables.tested_db  ) , recon_compare_tables(src_db_con.DS.get(db_tables.src_db),db_tables.src_db+".dbo."+db_tables.table_name,
                                 tested_db_con.DS.get(db_tables.tested_db), db_tables.tested_db+".dbo."+db_tables.table_name,
                                 keys, src_workflow_run_ids, tested_workflow_run_ids) ) )

    return result