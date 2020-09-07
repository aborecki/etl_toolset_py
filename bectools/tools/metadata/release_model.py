from bectools import connectors as con

from bectools import logger
import bectools.bec.data.sql.meta as sql_meta
import inspect

import logging


#DATASETS
def get_release_status_with_banks_pd_df(force_reload_from_source=False):
    def get_data():
        return con.ENV.get_nz_meta_db_connector().query_pandas(sql_meta.release_status_with_banks_sql())
    return con.CACHE.get_from_cache(__name__+"_"+inspect.currentframe().f_code.co_name, get_data, force_reload_from_source)

def get_release_status_pd_df(force_reload_from_source=False):
    def get_data():
        return con.ENV.get_nz_meta_db_connector().query_pandas(sql_meta.release_status_sql())
    return con.CACHE.get_from_cache(__name__+"_"+inspect.currentframe().f_code.co_name, get_data, force_reload_from_source)

#FILTERING FUNCTIONS


def get_release_status_table_get_ms_tablename(nz_tablename):
    release_status_table_meta=get_release_status_with_banks_pd_df()
    df=release_status_table_meta.query(f"NZ_TABLENAME=='{nz_tablename}'")
    res=set(list(df["MS_TABLENAME"]))
    if len(res)==0:
        raise Exception("No data found for NZ_TABLENAME=="+nz_tablename)
    if len(res)>1:
        raise Exception("More than one record found for NZ_TABLENAME==" + nz_tablename + " found:"+str(res))
    return list(res)[0]


def get_release_status_bank_i_for_table_and_koncern_bank(nz_tablename, koncern_bank_r, force_reload_from_source=False):
    df=get_release_status_with_banks_pd_df(force_reload_from_source)
    ret=df.query(f"NZ_TABLENAME=='{nz_tablename}' and KONCERN_BANK_R=={koncern_bank_r}")
    return (set(ret["BANK_I"]), ret)

def get_release_status_for_workflow_get_original_workflow_name(konv_workflow_name, force_reload_from_source=False):
    df=get_release_status_with_banks_pd_df(force_reload_from_source)
    ret=df.query(f"KONV_WORKFLOW_NAME=='{konv_workflow_name}'")
    return (set(ret["WORKFLOW_NAME"]), ret)

def get_release_status_koncern_banks_for_workflows(WFs, reload_from_source=False):

    df=get_release_status_with_banks_pd_df(reload_from_source)
    cond=" or ".join([f'KONV_WORKFLOW_NAME.str.contains("^{wf.replace("%",".*")}$")' for wf in  WFs])
    con.logger.debug(cond)
    df_filtered=df.query(cond, engine='python')
    return (set(df_filtered["KONCERN_BANK_R"]), df_filtered)


def get_inited_tables(workflow_names, reload_from_source=False):
    df=get_release_status_with_banks_pd_df(reload_from_source)
    cond=" or ".join([f'KONV_WORKFLOW_NAME.str.upper().str.contains("^{wf.upper().replace("%",".*")}$")' for wf in  workflow_names])
    cond=f"({cond}) and IS_INITABLE=='Y'"
    logger.debug(cond)
    df_filtered=df.query(cond, engine='python')
    return (set(df_filtered["NZ_TABLENAME"]), df_filtered)



def get_release_status_tables_per_bank(release):
    sql=sql_meta.release_status_tables_per_bank(release)
    logger.debug("SQL:\n"+sql)
    return con.ENV.get_nz_meta_db_connector().query_pandas(sql)



def get_release_status_table(table_name):
    return con.DB.FTST2.NZ.DS.MMDB.query_pandas(f"select * from release_status_table where nz_tablename like '{table_name}'")



def old_get_banks_for_wfs(workflow_names):
    if isinstance(workflow_names, str):
        workflow_names = [workflow_names]
    cond = "(" + " OR ".join(
        [f""" upper(WORKFLOW_NAME) LIKE UPPER('{workflow_name}') """ for workflow_name in workflow_names]) + ")"
    query=   f"""select DISTINCT lkp.KONCERN_BANK_R, lkp.BANK_I, lkp.BANK_R, lkp.KOERSEL_R
                                                    --, bs.VALID_FROM, bs.VALID_TO, bs.SOLUTION_I, WORKFLOW_NAME
                                                    from META.CNF_PWC_WORKFLOW wf 
                                                    INNER JOIN META.CNF_PWC_GROUP gr ON (wf.PWC_GROUP_I = gr.PWC_GROUP_I)
                                                    INNER JOIN META.CNF_BANK_SOLUTION bs ON (gr.SOLUTION_I = bs.SOLUTION_I)
                                                    INNER JOIN META.CNF_KONCERN lkp ON (lkp.KONCERN_BANK_R = bs.BANK_R)
                                                    WHERE {cond}
                                                    ORDER BY 1,2
                                                    """
    logger.debug(query)
    banks = con.DB.FTST2.NZ.DS.FTST2_META.query_pandas(query
      )
    # return bank df and map from concern bank to bank i

    return (banks, banks[["KONCERN_BANK_R","BANK_I"]].set_index('KONCERN_BANK_R').groupby('KONCERN_BANK_R')['BANK_I'].apply(list).to_dict())


release_status_table_meta=None
