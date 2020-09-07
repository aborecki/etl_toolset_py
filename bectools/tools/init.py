from collections import namedtuple
import bectools as bt

import pyetltools.tools.test  as test
from bectools import connectors as con
from pyetltools.data.pandas.tools import compare_data_frames
from time import perf_counter
import logging
from pyetltools.tools.misc import profile
from bectools import logger
import pandas as pd

TableMeta = namedtuple("TableMeta", "db schema table_name table_type")
TableCompareMeta=namedtuple("TableCompareMeta",TableMeta._fields+("condition","group_by_fields"))

def compare_inited_tables(tested_env, tables, banks, mode=1):

    ret = []
    list_tables_to_comp_src=[]
    list_tables_to_comp_tested=[]
    tested_db_con = con.DB.get(tested_env).NZ
    if tested_env=="FOPROD":
        tested_env="PROD"
    src_db_conn = con.DB.PROD.SQL.MDW
    for tb in sorted(set(tables)):
        for bnk in sorted(set(banks)):
            bnk_str=str(bnk).zfill(3)

            bank_is, _=bt.release_model.get_release_status_bank_i_for_table_and_koncern_bank(tb, bnk)

            tb_ms = bt.release_model.get_release_status_table_get_ms_tablename(tb)
            tb_meta = get_tables_meta(src_db_conn, tested_db_con, tb_ms, tb,
                                               "D00" + bnk_str + ".*MDW_PROD.*", ".*"+tested_env+"_MDW.*",
                                               tested_schema_regex="DBO")

            tb_meta_src = get_table_meta(src_db_conn, tb_ms,
                                               "D00" + bnk_str + ".*MDW_PROD.*")

            tb_meta_tested = get_table_meta(tested_db_con, tb, ".*"+tested_env+"_MDW.*","DBO")

            src_db_con=con.DB.PROD.SQL.MDW.with_data_source(tb_meta.src_db)

            tested_table_columns = set(bt.db_metadata.get_columns_of_object(tested_db_con.with_data_source(tb_meta.tested_db), tb)["COLUMN_NAME"])
            src_table_columns = set(bt.db_metadata.get_columns_of_object(src_db_con, tb_ms)["COLUMN_NAME"])

            group_by_fields_tested = []
            if "BANK_I" in tested_table_columns:
               cond_tested = "BANK_I IN (" + ",".join([str(bnk) for bnk in bank_is]) + ")"
               group_by_fields_tested=["BANK_I"]
               group_by_fields_tested = ["KONCERN_BANK_R"]
            elif "KONCERN_BANK_R" in tested_table_columns:
               cond_tested = f"KONCERN_BANK_R ='{bnk}'"
               group_by_fields_tested = ["KONCERN_BANK_R"]
            else:
               cond_tested="1=1"

            group_by_fields_src = []
            if "BANK_I" in src_table_columns:
               cond_src = "BANK_I IN (" + ",".join([str(bnk) for bnk in bank_is]) + ")"
               #group_by_fields_src=["BANK_I"]
            elif "KONCERN_BANK_R" in src_table_columns:
               cond_src= f"KONCERN_BANK_R ='{bnk}'"
               group_by_fields_src = ["KONCERN_BANK_R"]
            else:
               cond_src="1=1"

            if mode==2:
                list_tables_to_comp_src.append(TableCompareMeta( * tb_meta_src+ (cond_src, group_by_fields_src)))
                list_tables_to_comp_tested.append(TableCompareMeta(* tb_meta_tested +( cond_tested, group_by_fields_tested)))
            elif mode==1:
                result=compare_table_counts(src_db_con,
                                                     tb_meta.src_schema + "." + tb_meta.src_table_name,
                                                     tested_db_con.with_data_source(tb_meta.tested_db),
                                                     tb_meta.tested_schema + "." + tb_meta.tested_table_name, cond_src,
                                                     cond_tested)
                print(result)
                ret.append(result)
            else:
                print("mode unknown - 1 (compare one by one), 2 compare in batches")



    #return compare_table_counts_fast(con.DB.PROD.SQL.MDW, tested_db_con, list_tables_to_comp)

    if mode==1:
        init_comp_report = pd.concat(ret)
        return init_comp_report
    elif mode==2:
        return compare_table_counts_fast(src_db_con, tested_db_con, list_tables_to_comp_src, list_tables_to_comp_tested)



def get_objects(db_con, db_regex, reload_from_source=False):
    import re
    import pandas
    import os
    cache_key = ("DB_OBJECT_CACHE_"+db_con.key + "_" + db_regex)
    def get_databases():
        df=db_con.get_objects_for_databases([db for db in db_con.get_databases() if re.match(db_regex, db)])
        if df is None:
            print("No databases found for regex: "+db_regex)
        return df

    return con.CACHE.get_from_cache(cache_key, retriever=get_databases, force_reload_from_source= reload_from_source)


def get_table_dbs(db_con, tb_regex, db_regex):

    tbs=get_objects(db_con, db_regex)
    if tbs is None:
        print("No databases found. Check filtering regex for "+db_regex+" connection "+str(db_con))
        return None
    dbs=tbs[(tbs["NAME"].str.contains(tb_regex)) & ( ( tbs["TYPE"] == 'BASE TABLE')| (tbs["TYPE"] == 'TABLE')  | (tbs["TYPE"] == 'VIEW')) ]
    return dbs[["DATABASE_NAME","NAME","SCHEMA","TYPE"]]

def get_table_meta(db_con, tb_name, db_regex,  schema_regex=".*"):


    import re

    dbs=get_table_dbs(db_con, f"^{tb_name}$", db_regex)
    if dbs is not None:
        dbs=list(filter(lambda x: re.match(db_regex, x.DATABASE_NAME) and re.match(schema_regex, x.SCHEMA) ,
                        dbs.itertuples()))

    if len(dbs)==1 :
        return TableMeta(db=dbs[0].DATABASE_NAME,
                         schema=dbs[0].SCHEMA,
                         table_name = dbs[0].NAME,
                         table_type=dbs[0].TYPE
                         )
    else:
        print(f"Warning: Table:{tb_name} none or multiple dbs found src db:\n"+str(dbs) +  "\n Search parameters: src_db_regex:"+ db_regex + " schema_regex:"+schema_regex )
        return None




def get_tables_meta(src_db_con, tested_db_con,src_tb_name, tested_tb_name, src_db_regex, tested_db_regex,  src_schema_regex=".*", tested_schema_regex=".*"):

    TableMeta=namedtuple("TableMeta",["src_db", "tested_db", "src_schema","tested_schema","src_table_name", "tested_table_name"])
    import re

    src_dbs=get_table_dbs(src_db_con, f"^{src_tb_name}$", src_db_regex)
    if src_dbs is not None:
        src_dbs=list(filter(lambda x: re.match(src_db_regex, x.DATABASE_NAME) and re.match(src_schema_regex, x.SCHEMA) ,
                        src_dbs.itertuples()))


    tested_dbs = get_table_dbs(tested_db_con, f"^{tested_tb_name}$", tested_db_regex)

    if tested_dbs is not None:
        tested_dbs=list(filter(lambda x: re.match(tested_db_regex, x.DATABASE_NAME) and re.match(tested_schema_regex, x.SCHEMA),
                           tested_dbs.itertuples()))

    if len(src_dbs)==1 and len(tested_dbs)==1:
        return TableMeta(src_db=src_dbs[0].DATABASE_NAME, tested_db=tested_dbs[0].DATABASE_NAME,
                         src_schema=src_dbs[0].SCHEMA, tested_schema=tested_dbs[0].SCHEMA,
                         src_table_name = src_dbs[0].NAME, tested_table_name = tested_dbs[0].NAME)
    else:
        raise Exception(f"Table:SRC:{src_tb_name}/TESTED:{tested_tb_name}: none or multiple dbs found src db:\n"+str(src_dbs) + "  tested db:\n"+str(tested_dbs) +"\n src_db_regex:"+ src_db_regex + "tested_db_regex:"+tested_db_regex )




def get_table_counts_fast(db_con, tables):
    # Parameters:
    # db_con(DBConnector): database connector
    # tables(list): list of tuples
    # tuple fields:
    #  db schema table_name table_type condition group_by_fields

    def escape_sql(s):
        return s.replace("'", "''")

    tables_df=pd.DataFrame(tables)
    df_grouped = tables_df.groupby(["db", "table_name", "schema"]).agg(lambda x: list(x)).reset_index()
    import itertools
    group_by_all=set(itertools.chain.from_iterable([x.group_by_fields for x in tables]))

    sql = []
    for r in df_grouped.itertuples():

        cond = " OR  ".join(set([f"({c})" for c in r.condition]))

        if len(set([tuple(f) for f in r.group_by_fields])) > 1:
            raise Exception("Multiple group by fields for single table")

        group_by_select=",".join([f"null as {f}" if f not in r.group_by_fields[0] else f"{f} "  for f in  group_by_all])
        group_by= ",".join(r.group_by_fields[0])

        sql.append(
            f"""SELECT '{r.db}' DB,
                       '{r.table_name}' TABLE_NAME,
                       '{escape_sql(cond)}' WHERE_COND 
                        {(","+group_by_select) if group_by_select !='' else ''},
                        count(*) CNT  FROM {r.db}.{r.schema}.{r.table_name}
                  WHERE {cond if cond!='' else '1=1'} 
             {(" GROUP BY "+group_by) if group_by !='' else ''}""")
    sql = " UNION ALL \n".join(sql)


    ret=db_con.query_pandas(sql)
    return ret


def compare_table_counts_fast(src_db_con, tested_db_con, tables_src, tables_tested):
    counts_src=get_table_counts_fast(src_db_con, tables_src)
    counts_tested= get_table_counts_fast(tested_db_con, tables_tested)


    def match_rows(src, tested):
        tb_ms=bt.release_model.get_release_status_table_get_ms_tablename(tested.TABLE_NAME)
        if src.TABLE_NAME != tb_ms :
            return False
        if 'BANK_I' in src._fields and 'BANK_I' in tested._fields:
            if tested.BANK_I != src.BANK_I and tested.BANK_I is not None and src.BANK_I is not None:
                return False
        if 'KONCERN_BANK_R' in src._fields and 'KONCERN_BANK_R' in tested._fields:
            if tested.KONCERN_BANK_R != src.KONCERN_BANK_R and tested.KONCERN_BANK_R is not None and src.KONCERN_BANK_R is not None:
                return False
        if 'KONCERN_BANK_R' not in src._fields and 'KONCERN_BANK_R' in tested._fields:
            if tested.KONCERN_BANK_R is not None:
                koncern_bank= src.DB[3: 6]
                if koncern_bank!= str(int(tested.KONCERN_BANK_R)).zfill(3):
                    return False
        return True

    # implements nested join
    def join_nl(l_df,r_df):
        ret=[]

        i=0
        for l_t in l_df.itertuples():
            logger.info("Join progress: " + str( round(1.0*i/len(l_df)*100,2))+"%")
            i+=1
            l_t_matched=False
            for r_t in r_df.itertuples():
                if match_rows(l_t, r_t):
                    l_t_matched=True
                    ret.append(("BOTH",)+ l_t+r_t+( "FALSE" if l_t.CNT == r_t.CNT else "TRUE" , ))

            if not l_t_matched:
                ret.append(("SRC_ONLY",)+l_t + tuple([None]* len(r_t)) + (None,) )

        for r_t in r_df.itertuples():
            logger.info("Join progress (missing in left): " + str(round(1.0 * i / len(l_df) * 100, 2)) + "%")
            r_t_matched=False
            for l_t in l_df.itertuples():
                if match_rows(l_t, r_t):
                    r_t_matched=True

            if not r_t_matched:
                ret.append(("TESTED_ONLY",)+tuple([None]*len(l_t))+r_t+ (None,))

        return pd.DataFrame(ret, columns=list(["ROW_ORIGIN","IDX_SRC"]+[c+"_SRC" for c in  l_df.columns]) + list(["IDX_TESTED"]+[c+"_TESTED" for c in  r_df.columns])+  ["DIFF_ON_CNT_FLAG"])
    print("Preparing results.")
    import numpy as np
    src_df=counts_src.replace({np.nan: None})
    tested_df=counts_tested.replace({np.nan: None})
    ret=join_nl(src_df, tested_df)
    return  (ret,src_df,tested_df)


    def escape_sql(s):
        return s.replace("'","''")

    #left_df = left_db_connector.query_pandas(f"""select '{left_table_name}/{right_table_name}' TABLE_NAMES,'{left_db_connector.data_source}' DB,'{left_table_name}' TABLE_NAME, '{escape_sql(left_table_cond)}' WHERE_COND, count(*) CNT  from {left_table_name}
    #    where {left_table_cond}""")


    #right_df = right_db_connector.query_pandas(f"""select '{left_table_name}/{right_table_name}' TABLE_NAMES, '{right_db_connector.data_source}' DB,'{right_table_name}' TABLE_NAME, '{escape_sql(right_table_cond)}' WHERE_COND, count(*) CNT from {right_table_name}
    #    where {right_table_cond}""")

    #return compare_data_frames(left_df, right_df, ["TABLE_NAMES"], "LEFT", "RIGHT",
    #                    ["CNT"])


def compare_table_counts(left_db_connector, left_table_name,  right_db_connector, right_table_name, left_table_cond, right_table_cond):
    print("comparing table counts left/right:" + left_table_name + " " + right_table_name )
    print("comparing datasources left/right:" + left_db_connector.data_source + " " + right_db_connector.data_source)

    left_df = left_db_connector.query_pandas(f"""select '{left_table_name}/{right_table_name}' TABLE_NAMES,'{left_db_connector.data_source}' DB,'{left_table_name}' TABLE_NAME, '{escape_sql(left_table_cond)}' WHERE_COND, count(*) CNT  from {left_table_name}
        where {left_table_cond}""")

    right_df = right_db_connector.query_pandas(f"""select '{left_table_name}/{right_table_name}' TABLE_NAMES, '{right_db_connector.data_source}' DB,'{right_table_name}' TABLE_NAME, '{escape_sql(right_table_cond)}' WHERE_COND, count(*) CNT from {right_table_name}
        where {right_table_cond}""")

    return compare_data_frames(left_df, right_df, ["TABLE_NAMES"], "LEFT", "RIGHT",
                        ["CNT"])

def escape_sql(s):
    return s.replace("'","''")