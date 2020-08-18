from bectools import connectors as con
from collections import namedtuple
import tempfile
from pathlib import Path
import bectools as bt



def get_target_tables(workflow_name ):
    df=con.ENV.get_nz_meta_db_connector().query_pandas(
        f"select * from meta.wf_relations where upper(object_name) like upper('%{workflow_name}%') and table_type='TARGET'")
    return (set(df["TABLE_NAME"]), df)


def get_source_tables(workflow_name ):
    df=con.ENV.get_nz_meta_db_connector().query_pandas(
        f"select * from meta.wf_relations where upper(object_name) like upper('%{workflow_name}%') and table_type in ('SOURCE','LOOKUP')")
    return (set(df["TABLE_NAME"]), df)


def to_pandas(l, columns):
    import pandas
    d = pandas.DataFrame(l)
    d = d.rename(columns=dict([(i,c) for i,c in enumerate(columns)]))
    return d

def get_workflow_runs_ids(runs):
    run_ids = set(runs["WORKFLOW_RUN_ID"])
    return (run_ids, runs)


def get_search_condition(workflow_name, task_name=None, last_days=1, start_time_from=None, start_time_end=None):
    cond=f""" UPPER(WORKFLOW_NAME) LIKE UPPER('{workflow_name}') """
    if start_time_from:
        cond += f""" AND START_TIME >'{start_time_from}'"""
    if start_time_end:
        cond += f""" AND START_TIME <'{start_time_end}'"""
    if  start_time_end is None and start_time_end is None:
        cond += f""" AND START_TIME > DATEADD(D,-({last_days}-1),CAST(GETDATE() AS DATE))"""
        #--AND START_TIME > DATEADD(D,-{last_days},(SELECT MAX(CAST(START_TIME AS DATE) )  FROM BEC_TASK_INST_RUN WHERE UPPER(WORKFLOW_NAME) LIKE UPPER('%{workflow_name}%')))
    if task_name:
        cond += f""" AND TASK_NAME  LIKE '{task_name}'"""
    return cond

def search_workflow_task_runs(env, workflow_name, task_name=None, last_days=1, start_time_from=None, start_time_end=None):
    db_con=con.ENV.get_pc_rep_db_connector(env)

    cond=get_search_condition(workflow_name, task_name, last_days, start_time_from, start_time_end)
    query = f"""
        SELECT  DISTINCT CAST(START_TIME AS DATE) DATE_DAY,  WORKFLOW_RUN_ID, WORKFLOW_NAME, TASK_NAME, MAPPING_NAME, START_TIME, END_TIME, OPC_JOBNAME ,SERVER_NAME ,  SUBJECT_AREA,
        CASE WHEN ISNUMERIC(BANKNR)=1 THEN BANKNR ELSE '' END BANKNR, LOG_FILE, RUN_ERR_MSG, RUN_STATUS_CODE, SRCTGT_COUNT 
            FROM (SELECT *, RIGHT(WORKFLOW_NAME, CHARINDEX('_', REVERSE(WORKFLOW_NAME))-1 )  BANKNR FROM BEC_TASK_INST_RUN WHERE {cond}) x 
        ORDER BY 1,3,2
        """
    runs = db_con.query_pandas(query)
    ids= set(runs["WORKFLOW_RUN_ID"])
    return SearchResult(runs=runs, ids=ids)

SearchResult = namedtuple("SearchResult", "runs ids")
def search_workflow_runs(env, workflow_name, last_days=1, start_time_from=None, start_time_end=None):

    db_con=con.ENV.get_pc_rep_db_connector(env)
    cond = get_search_condition(workflow_name, None,last_days, start_time_from, start_time_end)
    query = f"""
        SELECT  DISTINCT CAST(MIN(START_TIME) AS DATE) DATE_DAY,  WORKFLOW_RUN_ID, WORKFLOW_NAME, MIN(START_TIME) MIN_START_TIME, MAX(END_TIME) MAX_END_TIME, OPC_JOBNAME ,SERVER_NAME ,  SUBJECT_AREA,
        MAX(CASE WHEN ISNUMERIC(BANKNR)=1 THEN BANKNR ELSE '' END) BANKNR, MAX(RUN_ERR_MSG) MAX_RUN_ERR_MSG , MAX(RUN_STATUS_CODE) MAX_RUN_STATUS_CODE, SUM(SRCTGT_COUNT) SUM_SRCTGT_COUNT 
            FROM (SELECT *, RIGHT(WORKFLOW_NAME, CHARINDEX('_', REVERSE(WORKFLOW_NAME))-1 )  BANKNR FROM BEC_TASK_INST_RUN WHERE {cond}) x 
        GROUP BY WORKFLOW_RUN_ID, WORKFLOW_NAME, CAST(START_TIME AS DATE),     
                 OPC_JOBNAME, SERVER_NAME, SUBJECT_AREA
                 ORDER BY 1,3,2"""
    runs = db_con.query_pandas(query)
    ids= set(runs["WORKFLOW_RUN_ID"])
    return SearchResult(runs=runs, ids=ids)


def compare_session_logs(left_env, right_env, wf_name_left, wf_name_right, task_name, output_dir, last_days=1, start_time_from=None, start_time_end=None):
    import os
    left_runs = search_workflow_task_runs(left_env, wf_name_left, task_name,
                                                 last_days, start_time_from, start_time_end)
    right_runs = search_workflow_task_runs(right_env, wf_name_right, task_name,
                                                last_days, start_time_from, start_time_end)
    left_logs = get_workflow_logs(left_runs.runs, use_converted_files=True,
                                         output_dir=os.path.join(output_dir,left_env))
    right_logs = get_workflow_logs(right_runs.runs, use_converted_files=True,
                                   output_dir=os.path.join(output_dir, right_env))

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
                    r.server_name, r.subject_area, r.workflow_run_id, r.workflow_name, wf_name_basis(r.workflow_name) workflow_name_basis, r.task_name, r.banknr,
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
                    r.server_name, r.subject_area, r.workflow_run_id, r.bankdato_date_param, r.workflow_name,r.workflow_name_basis, r.task_name, r.banknr, r.workflow_start_time,
                     timestamp, param_name, param_value 
                 from runs_spark_df r left outer join logs_spark_df p on (r.log_file = p.log_file and r.workflow_run_id = p.log_workflow_run_id) where  param_name is not null """)

    src_trg_df=con.SPARK.sql("""select 
                    r.server_name, r.subject_area, r.workflow_run_id, r.bankdato_date_param, r.workflow_name, r.workflow_name_basis,  r.task_name, r.banknr, r.workflow_start_time,  x.timestamp,
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
                    r.server_name, r.subject_area, r.workflow_run_id, r.bankdato_date_param, r.workflow_name, r.workflow_name_basis, r.task_name, r.banknr, r.workflow_start_time, x.timestamp,
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
    params_keys = ['workflow_name_basis','banknr', 'bankdato_date_param', 'task_name', 'param_name']
    params_comp=compare_data_frames(log_analysis_left.params, log_analysis_right.params, params_keys, left_name, right_name ,  ["param_value"])


    src_tgt_keys = ['workflow_name_basis', 'banknr','bankdato_date_param', 'task_name', 'src_trg_instance']
    src_tgt_comp=compare_data_frames(log_analysis_left.sources_targets, log_analysis_right.sources_targets, src_tgt_keys, left_name, right_name ,
                                     ["src_trg_output_rows","src_trg_rejected_rows" ])


    lookups_keys = ['workflow_name_basis', 'banknr', 'bankdato_date_param', 'task_name', 'lookup_trans']
    lookups_comp=compare_data_frames(log_analysis_left.lookups, log_analysis_right.lookups, lookups_keys, left_name, right_name ,  ["lookup_rows_count"])

    LogAnalysisComp = namedtuple("LogAnalysisComp", "left_runs right_runs  params_comp src_trg_comp lookups_comp left_name right_name")
    return LogAnalysisComp(log_analysis_left.runs, log_analysis_right.runs, params_comp, src_tgt_comp,  lookups_comp, left_name,  right_name)


def compare_data_frames(df_left, df_right, keys, left_name, right_name, cols_to_compare):
    import datacompy
    import pandas as pd

    left_name="_"+left_name
    right_name="_"+right_name


    compare = pd.merge(df_left, df_right, on=keys, suffixes=[left_name, right_name], indicator="row_source", how="outer")
    cols = list(df_left.columns)

    def sort_columns(c):
        cr = c.replace(left_name, "").replace(right_name, "")

        if cr in cols_to_compare:
            ret= cols_to_compare.index(cr)+ (1 if left_name in c else 0)
        else:
            ret= (cols.index(cr)+1) * 100 + (1 if left_name in c else 0)

        return ret

    cols = ["row_source"] + keys + sorted([col for col in compare if col not in keys + ["row_source"]], key=sort_columns)
    compare_srt = compare[cols]
    for col_comp in cols_to_compare:
        compare_srt[col_comp+"_diff"] = ~ (
                compare_srt[col_comp + left_name] == compare_srt[col_comp + right_name])
    return compare_srt

def save_log_analysis_to_excel(log_analysis, output_file):
    bt.pandas_helper.save_dataframes_to_excel(log_analysis, ["Runs","Params","Sources_Targets","Lookups"], output_file)

def save_log_analysis_comp_to_excel(log_analysis_comp, output_file):
    left_name = log_analysis_comp.left_name
    right_name = log_analysis_comp.right_name
    bt.pandas_helper.save_dataframes_to_excel(log_analysis_comp,
                                              [left_name+ " Runs" ,right_name + " Runs",
                                               left_name +" vs "+right_name + " Params",
                                               left_name +" vs "+right_name + " Src_Trg",
                                               left_name  +" vs "+right_name + " Lookups"], output_file)

def get_workflow_logs(runs, use_converted_files=True, overwrite_converted_files=False, output_dir = r"C:\out\temp"):
    import os
    import pandas

    res = None
    processed={}
    for r in runs.itertuples():
        #banknr = r.BANKNR
        log_file = r.LOG_FILE
        #workflow_name = r.WORKFLOW_NAME
        #workflow_run_id= r.WORKFLOW_RUN_ID
        #task_name=r.TASK_NAME
        #run_status_code=r.RUN_STATUS_CODE
        #server_name=r.SERVER_NAME
        #subject_area=r.SUBJECT_AREA

        if r.LOG_FILE:
            file_name = os.path.basename(log_file)
            output_file = os.path.join(output_dir, file_name + ".xml")
            print(output_file)
            if output_file in processed:
                print(output_file+ " already processed.")
                continue
            else:
                processed[output_file]=1
            Path(output_dir).mkdir(parents=True, exist_ok=True)

            log_df = con.INFA_CMD.FTST2.convert_log_bin_to_xml_and_parse(log_file, output_file=output_file,
                                                                         use_converted_files= use_converted_files,
                                                                         overwrite_converted_files=overwrite_converted_files,
                                                                         output_only_matched=True)[1];


            def add_columns(cols):
                for i,c in enumerate(cols):
                    log_df.insert(i, c[0],c[1])
                    log_df[c[0]] = c[1]

            add_columns(
                [
                    ("log_file",log_file)
                   # ("workflow_name", workflow_name),
                   # ("banknr", banknr),
                   # ("workflow_run_id", workflow_run_id),
                   # ("task_name", task_name),
                   # ("run_status_code", run_status_code),
                   # ("server_name", server_name),
                   # ("subject_area", subject_area)
                ]
            )

            if res is None:
                res = log_df
            else:
                #res = tuple( pandas.concat([res, df])  for res, df in zip(res,(log_df,) ) )
                res = pandas.concat([res, log_df])
    Logs=namedtuple("Logs","runs logs")
    return Logs(runs, res)


def get_workflow_runs_for_today(WF_NAME):
    prod_runs = search_workflow_task_runs(con.DB.PROD.SQL.PC_REP, WF_NAME,
                                                start_time_from='2020-08-06', start_time_end="2020-08-07")

def compare_table_counts(table_df, src_db_con, tested_db_con, banks, src_db_suffix, tested_db_suffix):

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

    return con.SPARK.spark_helper.compare_dataframes(left_df, right_df,
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


db_objects_meta=dict()

def reset_object_cache():
    global db_objects_meta
    db_objects_meta = dict()
    import os
    f = []
    files_to_del= [os.path.join(tempfile.gettempdir(),x) for x in os.listdir(tempfile.gettempdir()) if x.startswith("DB_OBJECT_CACHE_")]
    for f in files_to_del:
        print("Removed "+str(f))
        os.remove(f)

def get_objects(db_con, db_suffix):
    import pandas
    import os
    cache_key = ("DB_OBJECT_CACHE_"+db_con.key + "_" + db_suffix).replace("/","_")
    if cache_key in db_objects_meta:
        return db_objects_meta
    else:
        file = os.path.join(tempfile.gettempdir(), cache_key + ".parquet")
        try:
            print("Reading file from cache")
            df=pandas.read_parquet(file)
            db_objects_meta[cache_key]=df
            return df
        except Exception as e:
            print("Cannot read file from cache.")
        df=db_con.get_objects_for_databases([db for db in db_con.get_databases_by_name_part('D00') if db.endswith(db_suffix)])
        df.to_parquet(file)
        db_objects_meta[cache_key]=df
        return df

def get_table_dbs(db_con, tb_name, db_suffix):
    if db_con not in db_objects_meta:
        print("Getting objects")
        db_objects_meta[db_con]=get_objects(db_con, db_suffix)
    tbs= db_objects_meta[db_con]
    if tbs is None:
        print("No databases found. Check prefix for connection "+str(db_con))
        return set()
    dbs=tbs[(tbs["TABLE_NAME"] == tb_name) & ( (tbs["TABLE_TYPE"] == 'BASE TABLE') | (tbs["TABLE_TYPE"] == 'VIEW')) ]
    return set(dbs["DATABASE_NAME"])


def get_tables_meta(src_db_con, tested_db_con,tb_name, bank_nr, src_db_suffix, tested_db_suffix):
    TableMeta=namedtuple("TableMeta",["src_db", "tested_db","table_name", "bank_nr"])


    db_prefix="D00"+bank_nr.zfill(3)
    src_dbs=list(filter(lambda x: x.startswith(db_prefix) ,get_table_dbs(src_db_con, tb_name, src_db_suffix)))
    tested_dbs=list(filter(lambda x: x.startswith(db_prefix),get_table_dbs(tested_db_con, tb_name, tested_db_suffix)))

    if len(src_dbs)==1  and len(tested_dbs)==1:
        return TableMeta(src_db=src_dbs[0], tested_db=tested_dbs[0], table_name = tb_name, bank_nr=bank_nr)
    else:
        raise Exception(f"{tb_name}: none or multiple dbs found src db:"+str(src_dbs) + "  tested db:"+str(tested_dbs) )



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
            db_tables=get_tables_meta(src_db_con, tested_db_con, tb, bnk, src_db_suffix, tested_db_suffix)
            keys=[]
            if tb not in table_keys:
                print(tb+ " not in table_keys dict" )
            else:
                keys=table_keys[tb]
            result.append( ( (tb, bnk,db_tables.src_db, db_tables.tested_db  ) , recon_compare_tables(src_db_con.DS.get(db_tables.src_db),db_tables.src_db+".dbo."+db_tables.table_name,
                                 tested_db_con.DS.get(db_tables.tested_db), db_tables.tested_db+".dbo."+db_tables.table_name,
                                 keys, src_workflow_run_ids, tested_workflow_run_ids) ) )

    return result