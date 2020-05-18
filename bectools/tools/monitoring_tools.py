from bectools import connectors as con

def get_lkp_koncern_pd_df():
    return con.DB.FTST2.NZ.DS.FTST2_META.run_query_pandas_dataframe("select * from meta.CNF_KONCERN")

def get_bec_task_inst_run_sql(workflow_name, start_time, where_cond="1=1"):
    """

    :param workflow_name: Wildcard name of the workflow(s) to return
    :param start_time: cut off date time
    :return: Text of sql query
    """
    return f"""
    SELECT 
           subject_area as folder_name, 
           workflow_name_short as workflow_name, 
           workflow_name AS cloned_workflow_name, 
           opc_jobname   AS pwc_opb_jobname, 
           koersel_bank,
           Concat('___',Substring(opc_jobname, 3, 3), 
           Replace(Substring(opc_jobname, 6, 3), 
                  koersel_bank, '_')) 
                         common_pwc_opb_jobname,           
           *
    FROM   (SELECT *, 
                   Substring(workflow_name, 
                   Len(workflow_name) - koersel_bank_seprator_index + 2, 
                   Len(workflow_name)) koersel_bank, 
                   Substring(workflow_name, 1, 
                   Len(workflow_name) - koersel_bank_seprator_index) 
                                       workflow_name_short 
            FROM   (SELECT *, 
                           Charindex('_', Reverse(workflow_name)) 
                           koersel_bank_seprator_index 
                    FROM   [D00000PD10_PWC_REP_PROD].[DBO].[bec_task_inst_run] 
                    WHERE  {where_cond}
                           AND start_time > '{start_time}' 
                           AND workflow_name NOT LIKE '%PUSH' 
                           AND upper(workflow_name) like upper('{workflow_name}')
                        ) x) y 
                """

def get_bec_task_inst_run_agg_workflow():
    pass

def get_bec_task_inst_run_prod_pd_df(workflow_name, start_time="2020-01-01"):
    return con.DB.PROD.SQL.PC_REP.DS.D00000PD10_PWC_REP_PROD.run_query_pandas_dataframe(
        get_bec_task_inst_run_sql(workflow_name, start_time))


