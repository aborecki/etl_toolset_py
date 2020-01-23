from copy import copy, deepcopy

import pyodbc
import pyspark
import pandas
from pyspark.sql.types import *

from pyetltools.core import context
from pyetltools.data.spark import spark_helper
from pyetltools.data.config import DBConfig, ServerType
from pyetltools.data.core.connection import DBConnection
from pyetltools.data.core.nz_db_connection import NZDBConnection
from pyetltools.data.core.sql_server_db_connection import SQLServerDBConnection


def df_to_excel(filename):
    spark_helper.df_to_excel(filename)


def df_to_csv(dir):
    spark_helper.df_to_csv(dir)


class DBContext:
    def __init__(self, config: DBConfig, connection: DBConnection):
        self.config = config
        self.connection = connection
        self.spark_context= context.get("SPARK")

    def set_data_source(self, data_source):
        self.connection.set_data_source(data_source)
        return self

    def run_query_spark_dataframe(self, query):
        if self.connection.supports_jdbc():
            df =  self.spark_context.get_spark_session().read.format("jdbc") \
                    .option("url", self.connection.get_jdbc_conn_string()) \
                    .option("dbtable", "(" + query + ") x") \
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                    .load();

            df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
            # is_empty = df.count() == 0
            return df
        else:
                pdf = self.get_pandas_dataframe(query)
                is_empty = pdf.empty
                if not is_empty:
                    return spark_helper.pandas_to_spark(self.spark_context.get_spark_session(), pdf)


    def run_query_pandas_dataframe(self, query):
        conn = pyodbc.connect(self.connection.get_odbc_conn_string())
        return pandas.read_sql(query, conn)

    def execute_statement(self, statement):
        conn = pyodbc.connect(self.connection.get_odbc_conn_string());
        cursor = conn.cursor();
        cursor.execute(statement)
        res = []
        print("rowcount:"+str(cursor.rowcount))
        try:
            res.append(cursor.fetchall())
            for row in cursor.fetchall():
                print(row)
        except pyodbc.ProgrammingError:
            pass
        while cursor.nextset():  # NB: This always skips the first resultset
            try:
                res.append(cursor.fetchall())
                for row in cursor.fetchall():
                    print(row)
                # break
            except pyodbc.ProgrammingError:
                continue
        return res;

    def get_databases(self):
        return self.run_query_pandas_dataframe(self.connection.get_sql_list_databases())

    def get_objects(self):
        return self.run_query_pandas_dataframe(self.connection.get_sql_list_objects())

    @classmethod
    def create_connection_from_config(cls, config: DBConfig):
        if config.db_type == ServerType.NZ:
            return  NZDBConnection(config)
        if config.db_type == ServerType.SQLSERVER:
            return  SQLServerDBConnection(config)

    @classmethod
    def create_from_config(cls, config: DBConfig):
        return DBContext(config, cls.create_connection_from_config(config))


    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(DBConfig, DBContext)
