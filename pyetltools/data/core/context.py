from copy import copy, deepcopy

import pyodbc
import pyspark
import pandas
from pyspark.sql.types import *

from pyetltools.config.config import Config
from pyetltools.core import context
from pyetltools.core.context import Container
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
from sqlalchemy.orm import sessionmaker


class DBContext:
    def __init__(self, config: DBConfig, connection: DBConnection):
        self.config = config
        self.connection = connection
        self._spark_context = None
        self.DS=Container()

        if config.load_db_contexts:
            self.load_db_sub_contexts()


    @property
    def spark_context(self):
        # implements lazy load of spark context to speed up library initialization
        if self._spark_context is None:
            self._spark_context=context.get(self.config.spark_context)
        return self._spark_context

    def set_data_source(self, data_source):
        self.connection.set_data_source(data_source)
        return self

    def run_query_spark_dataframe(self, query,  registerTempTableName=None):
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



    def sa_get_engine(self):
        con_str = self.connection.get_sqlalchemy_conn_string()
        return sa.create_engine(con_str)

    def sa_connect(self):
        return self.sa_get_engine().connect();

    def sa_get_session_class(self):
        return sessionmaker(bind=self.sa_get_engine())

    def sa_get_session(self):
        # create object of the sqlalchemy class
        return self.sa_get_session_class()()

    def sa_get_metadata_for_table(self, table):
        from sqlalchemy import MetaData
        metadata = MetaData(bind=self.sa_get_engine(), reflect=False)
        schema_table_split=table.split(".")
        # take last part as tablename and part before as schema
        schema=None
        if len(schema_table_split)>1:
            _table=schema_table_split[-1]
            _schema=schema_table_split[-2]
        metadata.reflect(only=[_table], schema=_schema)
        return metadata.tables[table]


    def sa_helper_print_object_template(self, table):
        table_meta_data=self.sa_get_metadata_for_table(table)
        ret = []
        for i in table_meta_data.columns:
            x = repr(i)
            ss = x.split(",")
            ret.append(i.name.lower() + "= Column(" + ss[1] + ")")
        print("\n".join(ret))

        ret = []
        for i in table_meta_data.columns:
            ret.append("'" + i.name.lower() + "'")
        print("__ordering=[" + ",".join(ret) + "]")

    def get_databases(self):
        ret = self.run_query_pandas_dataframe(self.connection.get_sql_list_databases())
        ret.columns = ret.columns.str.upper()
        return list(ret["NAME"])

    def get_object_by_name_regex(self, regex):
        objects=self.run_query_pandas_dataframe(self.connection.get_sql_list_objects())
        objects.columns = map(str.lower, objects.columns)
        return objects[objects.name.str.match(regex)]

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

    def get_context_for_each_database(self):
        ret={}
        for db in self.get_databases():
            new_conf = DBConfig(template=self.config, data_source=db, load_db_contexts=False)
            ctx=self.create_from_config(new_conf)
            ret[db] = ctx
        return ret

    def load_db_sub_contexts(self):
        """
        Adds database contexts for each database
        :return:
        """
        for db, ctx in self.get_context_for_each_database().items():
            if not hasattr(self.DS, db):
                setattr(self.DS, db, ctx)


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

    @classmethod
    def df_to_excel(filename):
        spark_helper.df_to_excel(filename)

    @classmethod
    def df_to_csv(dir):
        spark_helper.df_to_csv(dir)

