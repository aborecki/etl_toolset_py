from pyetltools import get_default_logger
from pyetltools.core.env_manager import get_default_cache
from pyetltools.tools.misc import CachedDecorator, RetryDecorator
import pandas as pd
import re


def get_databases_cached(db_con, force_reload_from_source=False, days_in_cache=999999):
    cache_key = ("DB_DATABASES_CACHE_" + db_con.key + "_" + db_con.data_source)

    @RetryDecorator(manual_retry=False, fail_on_error=False)
    @CachedDecorator(cache=get_default_cache(),cache_key=cache_key, force_reload_from_source=force_reload_from_source, days_in_cache=days_in_cache)
    def get_databases():

        df=db_con.get_databases()
        return df
    return get_databases()

def get_objects_cached(db_con, force_reload_from_source=False, days_in_cache=999999):

    cache_key = ("DB_OBJECT_CACHE_" + db_con.key + "_" + db_con.data_source)

    @RetryDecorator(manual_retry=False, fail_on_error=False)
    @CachedDecorator(cache=get_default_cache(),cache_key=cache_key, force_reload_from_source=force_reload_from_source, days_in_cache=days_in_cache)
    def get_objects():

        df=db_con.get_objects()
        if df is None:
            raise Exception("No databases found")
        return df
    return get_objects()

def get_objects_for_multiple_dbs(db_con, db_name_regex=".*", **kwargs):
    ret=[]
    dbs=[db for db in get_databases_cached(db_con, **kwargs) if re.match(db_name_regex, db)]
    if len(dbs)==0:
        get_default_logger().warn("No databases found.")
        return None

    for db in dbs:
        df = get_objects_cached(db_con.with_data_source(db), **kwargs)
        if df is None:
            get_default_logger().warn("No objects found in "+db)
        else:
            df["DATABASE_NAME"]=db
            ret.append(df)
    if len(ret)==0:
        get_default_logger().warn("No objects found")
        return None
    else:
        return pd.concat(ret)

def get_objects_by_name(db_con, object_name_regex=".*", db_name_regex=".*", **kwargs):
    if db_name_regex:
        df = get_objects_for_multiple_dbs(db_con, db_name_regex,  **kwargs)
    else:
        df= get_objects_cached(db_con,  **kwargs)
    if df is not None:
        return df.query(f"NAME.str.upper()==('{object_name_regex.upper()}')")

def get_objects_by_name_regex(db_con, object_name_regex,  db_name_regex=".*", **kwargs):
    df = get_objects_for_multiple_dbs(db_con, db_name_regex,  **kwargs)
    if df is not None:
        return df.query(f"NAME.str.upper().str.match('{object_name_regex.upper()}')")

# def get_objects(db_con, db_regex, force_reload_from_source=False):
#     cache_key = ("DB_OBJECT_CACHE_"+db_con.key+"_"+db_con.data_source + "_" + db_regex)
#     def get_objects():
#         df=db_con.get_objects_for_databases([db for db in db_con.get_databases() if re.match(db_regex, db)])
#         if df is None:
#             print("No databases found for regex: "+db_regex)
#         return df
#
#     return get_cache().get_from_cache(cache_key, retriever=get_objects, force_reload_from_source= force_reload_from_source)


def get_columns(db_con, force_reload_from_source=False, days_in_cache=999999):
    cache_key = ("DB_OBJECT_COLUMN_CACHE_" + db_con.key + "_" + db_con.data_source )

    @RetryDecorator(manual_retry=False, fail_on_error=False)
    @CachedDecorator(cache=get_default_cache(), cache_key=cache_key, force_reload_from_source=force_reload_from_source,
                     days_in_cache=days_in_cache)
    def get_columns():
        df = db_con.get_columns_all_objects()
        if df is None:
            print("No columns found")
        else:
            df.columns = map(str.upper, df.columns)
        return df

    return get_columns()

def get_columns_for_multiple_dbs(db_con, db_name_regex=".*" ,**kwargs):
    ret=[]
    dbs=[db for db in get_databases_cached(db_con, **kwargs) if re.match(db_name_regex, db)]
    if len(dbs)==0:
        get_default_logger().warn("No databases found.")
        return None

    for db in dbs:
        df = get_columns(db_con.with_data_source(db), **kwargs)
        if df is None:
            get_default_logger().warn("No columns found in "+db)
        else:
            df["DATABASE_NAME"]=db
            ret.append(df)
    if len(ret)==0:
        get_default_logger().warn("No columns found.")
        return None
    else:
        return pd.concat(ret)


@CachedDecorator()
def get_columns_by_object_name(db_con, object_name, db_name_regex=".*", **kwargs):
    df= get_columns_for_multiple_dbs(db_con, db_name_regex, **kwargs)
    return df.query(f"TABLE_NAME.str.upper()== '{object_name.upper()}'").sort_values("ORDINAL_POSITION")

def get_columns_by_object_name_regex(db_con, object_name_regex, db_name_regex=".*", **kwargs):
    df= get_columns_for_multiple_dbs(db_con, db_name_regex, **kwargs)
    return df.query(f"TABLE_NAME.str.upper().str.match('{object_name_regex.upper()}')").sort_values("ORDINAL_POSITION")

def get_tables_metadata(db_con, tables, input_columns=["TABLE_NAME_REGEX","DB_NAME_REGEX"], output_columns=["NAME","SCHEMA","DATABASE_NAME"]):
    """
        Searches for metadata for tables, input tables data frame should contains fields: table_name_regex and DB_NAME_REGEX
        Result of the search will be added to the input dataframe in columns: NAME, SCHEMA, DATABASE_NAME
    """

    if any(col in tables.columns for col in output_columns):
        raise Exception("Output column name already in data frame")

    if len(output_columns)!=3:
        raise Exception("output_columns parameters should contain 3 values: for name, schema and database name")

    def add_db_meta(db_con):
        def get_db_meta(df):
            r=df.iloc[0]
            db = r[df.columns.get_loc(input_columns[1])]
            table_name = r[df.columns.get_loc(input_columns[0])]
            get_default_logger().info("Getting table meta for: "+table_name + " db:" + db)
            meta = db_con.get_objects_by_name_regex(f"{table_name}", db)
            if meta is None or len(meta) == 0:
                print("Not found")
                r[output_columns[0]] = None
                r[output_columns[1]] = None
                r[output_columns[2]] = None
                return pd.DataFrame([r])
            else:
                if len(meta)>1:
                    print("Find multiple matches")
                res=[]
                for m in meta.itertuples():
                    r_cp=r.copy()
                    r_cp[output_columns[0]] = m.NAME
                    r_cp[output_columns[1]] = m.SCHEMA
                    r_cp[output_columns[2]] = m.DATABASE_NAME
                    res.append(r_cp)
                return pd.DataFrame(res)

        return get_db_meta

    return tables.groupby(tables.columns.tolist(), group_keys=False).apply(add_db_meta(db_con))

def get_table_counts(db_con, tables, agg_queries=True):
    # Parameters:
    # db_con(DBConnector): database connector
    # tables(list): list of tuples
    # tuple fields:
    #  db schema table_name table_type condition group_by_fields

    def escape_sql(s):
        if s:
            return s.replace("'", "''")
        else:
            return ""

    sqls = []
    ret=[]

    if len(tables)==0:
        ret= pd.DataFrame(columns=["DATABASE_NAME","SCHEMA","NAME","WHERE_COND","CNT"])
    for r in tables.itertuples():

        cond=None
        if "CONDITION" in tables:
            cond = r.CONDITION
        group_by=None
        if "GROUP_BY" in tables:
            group_by=r.GROUP_BY

        sql=f"""SELECT '{r.DATABASE_NAME}' DATABASE_NAME,
                        '{r.SCHEMA}' "SCHEMA",
                       '{r.NAME}' NAME,
                       '{escape_sql(cond)}' WHERE_COND 
                        {(","+group_by) if group_by else ''},
                        count(*) CNT  FROM {r.DATABASE_NAME}.{r.SCHEMA}.{r.NAME}
                  WHERE {cond if cond else '1=1'} 
             {(" GROUP BY "+group_by) if group_by else ''}"""

        if not agg_queries:
            ret.append(db_con.query_pandas(sql))
        else:
            sqls.append(sql)


    if  agg_queries:
        sql = " UNION ALL \n".join(sqls)
        ret=[db_con.query_pandas(sql)]
    return pd.concat(ret)


