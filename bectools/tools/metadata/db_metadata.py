from bectools import connectors as con

def get_columns(db_con):
    cache_key = ("DB_OBJECT_COLUMN_CACHE_" + db_con.key + "_" + db_con.data_source )

    def get_columns():
        df = db_con.get_columns_all_objects()
        if df is None:
            print("No columns found")
        df.columns = map(str.upper, df.columns)
        return df

    return con.CACHE.get_from_cache(cache_key, retriever=get_columns)

def get_columns_of_object(db_con, table_name):
    df= get_columns(db_con)
    return df.query(f"TABLE_NAME.str.upper()== '{table_name.upper()}'").sort_values("ORDINAL_POSITION")