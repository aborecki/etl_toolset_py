from copy import copy, deepcopy

import pyodbc
import pyspark
import pandas
from pyspark.sql.types import *

from pyetltools.core import context
from pyetltools.data.core.db2_connection import DB2DBConnection
from pyetltools.data.spark import spark_helper
from pyetltools.data.config import DBConfig, ServerType
from pyetltools.data.core.connection import DBConnection
from pyetltools.data.core.nz_db_connection import NZDBConnection
from pyetltools.data.core.sql_server_db_connection import SQLServerDBConnection
from sys import stderr
import sqlalchemy as sa
import urllib.parse
import pyetltools.data.core.sqlalchemy.netezza_dialect


def df_to_excel(filename):
    spark_helper.df_to_excel(filename)


def df_to_csv(dir):
    spark_helper.df_to_csv(dir)


class DBContext:
    def __init__(self, config: DBConfig, connection: DBConnection):
        self.config = config
        self.connection = connection
        self.spark_context = context.get("SPARK")

    def set_data_source(self, data_source):
        self.connection.set_data_source(data_source)
        return self

    def run_query_spark_dataframe(self, query, registerTempTableName=None):
        if self.connection.supports_jdbc():
            cf = self.spark_context.get_spark_session().read.format("jdbc") \
                .option("url", self.connection.get_jdbc_conn_string()) \
                .option("dbtable", "(" + query + ") x") \
                .option("driver", self.connection.get_jdbc_driver())
            if not self.config.integrated_security:
                cf.option("user", self.config.username) \
                    .option("password", self.connection.get_password())
            df = cf.load()

            df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)

            ret = df
        else:
            pdf = self.run_query_pandas_dataframe(query)
            is_empty = pdf.empty
            if not is_empty:
                ret = spark_helper.pandas_to_spark(self.spark_context.get_spark_session(), pdf)
        if registerTempTableName is not None:
            ret.registerTempTable(registerTempTableName)
        return ret

    def run_query_pandas_dataframe(self, query):
        conn = pyodbc.connect(self.connection.get_odbc_conn_string())
        return pandas.read_sql(query, conn, coerce_float=False, parse_dates=None)

    def execute_statement(self, statement):
        conn = pyodbc.connect(self.connection.get_odbc_conn_string())
        cursor = conn.cursor()
        cursor.execute(statement)
        res = []
        print("rowcount:" + str(cursor.rowcount))
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

    def connect_sqlchemy(self):

        con_str = '{}+pyodbc:///?odbc_connect={}'.format(self.connection.get_sqlalchemy_dialect(),
                                                         urllib.parse.quote_plus(self.connection.get_odbc_conn_string()))
        print(con_str)
        engine = sa.create_engine(con_str)
        return engine.connect();

    def get_databases(self):
        ret = self.run_query_pandas_dataframe(self.connection.get_sql_list_databases())
        ret.columns = ret.columns.str.upper()
        return list(ret["NAME"])

    def get_objects(self):
        return self.run_query_pandas_dataframe(self.connection.get_sql_list_objects())

    def get_columns(self, table_name):
        return self.run_query_pandas_dataframe(self.connection.get_sql_list_columns(table_name))

    def get_objects_for_databases(self, databases):
        orig_ds = self.connection.data_source
        ret = None
        for db in databases:
            try:
                print("Retrieving objects from: " + db)
                self.connection.set_data_source(db)
                ret_tmp = self.run_query_pandas_dataframe(self.connection.get_sql_list_objects())
                ret_tmp["DATABASE_NAME"] = db
                if ret is None:
                    ret = ret_tmp
                else:
                    ret = ret.append(ret_tmp)
            except Exception as e:

                print(e, file=stderr)
        self.connection.set_data_source(orig_ds)
        return ret;

    def get_object_for_databases_single_query(self, databases):
        return self.run_query_pandas_dataframe(
            self.connection.get_sql_list_objects_from_datbases_single_query(databases))

    @classmethod
    def create_connection_from_config(cls, config: DBConfig):
        if config.db_type == ServerType.NZ:
            return NZDBConnection(config)
        if config.db_type == ServerType.SQLSERVER:
            return SQLServerDBConnection(config)
        if config.db_type == ServerType.DB2:
            return DB2DBConnection(config)
        raise Exception("Unknown db type:" + str(config.db_type))

    @classmethod
    def create_from_config(cls, config: DBConfig):
        return DBContext(config, cls.create_connection_from_config(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(DBConfig, DBContext)
