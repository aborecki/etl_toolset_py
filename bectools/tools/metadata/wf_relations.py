import inspect

from bectools import connectors as con

import bectools.bec.data.sql.meta as sql_meta

#DATASETS
def get_wf_relations(env="FTST2", force_reload_from_source=False):
    def get_data():
        return con.ENV.get_nz_meta_db_connector(env).query_pandas(
            f"select * from meta.wf_relations where and table_type in ('SOURCE','LOOKUP')")
    return con.CACHE.get_from_cache(__name__+inspect.currentframe().f_code.co_name+"_"+env+"_", get_data, force_reload_from_source)

def get_source_tables(env, workflow_name):
    df=get_wf_relations(env)
    df=df.query(f"OBJECT_NAME.str.contains('{workflow_name}') and (TABLE_TYPE == 'SOURCE' or TABLE_TYPE == 'TARGET')")
    return (set(df["TABLE_NAME"]), df)


def get_target_tables(workflow_name ):
    df=get_wf_relations(env)
    df=df.query(f"""OBJECT_NAME.str.upper().contains('{workflow_name}') and (TABLE_TYPE == 'SOURCE' or TABLE_TYPE == 'TARGET')
                AND TABLE_NAME != 'DUMMY_TABLE_FOR_APPLY' AND TABLE_NAME != 'TDM_LOAD_LOG' AND TABLE_NAME != 'DUMMY_NUL_TRG' AND
                TABLE_NAME !='FLAT_FILE'
                """)
    return (set(df["TABLE_NAME"]), df)
