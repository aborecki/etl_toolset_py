

def build_query(table_name, where=[], limit_rows=None):
    lmt = ""
    if limit_rows is not None:
        lmt = f"top {limit_rows}"
    sql = f"select {lmt} * from {table_name} where 1=1\n"
    for p in where:
        sql = sql + f" and {p} \n"
    return sql

def sql_pwc_rep_prod_BEC_TASK_INST_RUN(where=[], limit_rows=None):

    return build_query("[D00000PD10_PWC_REP_PROD].[dbo].[BEC_TASK_INST_RUN]",where, limit_rows)

def sql_pwc_rep_prod_REP_WFLOW_RUN_INST(where=[], limit_rows=None):
    return build_query("[D00000TD10_PWC_REP_FTST2].[dbo].[REP_WFLOW_RUN_INST]", where, limit_rows)


