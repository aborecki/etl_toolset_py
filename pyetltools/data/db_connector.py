import copy
from abc import abstractmethod

import pyodbc
import pandas

from pyetltools.core import connector
from pyetltools.core.attr_dict import AttrDict
from pyetltools.core.connector import Connector
from pyetltools.data.spark import spark_helper
from sys import stderr
import sqlalchemy as sa
import urllib.parse
from sqlalchemy.orm import sessionmaker


class DBConnector(Connector):
    def __init__(self,
                 key=None,
                 host=None,
                 port=None,
                 dsn=None,
                 username=None,
                 password=None,
                 data_source=None,
                 odbc_driver=None,
                 integrated_security=None,
                 supports_jdbc=None,
                 supports_odbc=None,
                 load_db_connectors=None,
                 spark_connector_key="SPARK",
                 db_dialect_class=None
                 ):
        super().__init__(key=key)
        assert key, "Config key cannot be None"
        self.host = host
        self.port = port
        self.dsn = dsn
        self.username = username
        self.password = password
        self.data_source = data_source
        self.odbc_driver = odbc_driver
        self.integrated_security = integrated_security
        self.supports_jdbc = supports_jdbc
        self.supports_odbc = supports_odbc
        self.load_db_connectors = load_db_connectors
        self.spark_connector_key = spark_connector_key
        self.db_dialect_class = db_dialect_class


        self.db_dialect=self.db_dialect_class()
        self.DS=AttrDict(initializer=lambda _: self.load_db_sub_connectors() )

        if self.load_db_connectors:
            self.load_db_sub_connectors()

    def get_spark_connector(self):
        return connector.get(self.spark_connector_key)

    def set_data_source(self, data_source):
        self.data_source=data_source
        return self

    def clone_set_data_source(self, data_source):
        new_conn=copy.copy(self)
        new_conn.config = copy.copy(self)
        new_conn.set_data_source(data_source)
        return new_conn

    def get_password(self):
        if self.integrated_security:
            return None
        else:
            return super().get_password()

    def get_odbc_conn_string(self):
        return self.db_dialect.get_odbc_conn_string(
            self.dsn,
            self.host,
            self.port,
            self.data_source,
            self.username,
            self.get_password, #passing as callback so function will use it only if needed
            self.odbc_driver,
            self.integrated_security)

    def get_jdbc_conn_string(self):
        return self.db_dialect.get_jdbc_conn_string(
            self.dsn,
            self.host,
            self.port,
            self.data_source,
            self.username,
            self.get_password,  #passing as callback so function will use it only if needed
            self.odbc_driver,
            self.integrated_security)

    def get_odbc_connection(self):
        pyodbc.connect(self.get_odbc_conn_string())


    def run_query_spark_dataframe(self, query,  registerTempTableName=None):
        if self.supports_jdbc:
            ret=self.get_spark_connector().get_df_from_jdbc(self.get_jdbc_conn_string(),
                                                      query,
                                                      self.db_dialect.get_jdbc_driver(),
                                                      self.username,
                                                      self.get_password)
        else:
            print("Warning: conversion from panadas df to spark needed. Can be slow.")
            pdf = self.run_query_pandas_dataframe(query)
            is_empty = pdf.empty
            if not is_empty:
                ret = spark_helper.pandas_to_spark(self.get_spark_connector().get_spark_session(), pdf)
        if registerTempTableName is not None:
            ret.registerTempTable(registerTempTableName)
        return ret

    def run_query_pandas_dataframe(self, query):
        conn = pyodbc.connect(self.get_odbc_conn_string())
        return pandas.read_sql(query, conn, coerce_float=False, parse_dates=None)

    def execute_statement(self, statement):
        conn = pyodbc.connect(self.get_odbc_conn_string())
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
        con_str = self.db_dialect.get_sqlalchemy_conn_string()
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
        _table = table
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
        ret = self.run_query_pandas_dataframe(self.db_dialect.get_sql_list_databases())
        ret.columns = ret.columns.str.upper()
        return list(ret["NAME"])

    def get_object_by_name_regex(self, regex):
        objects=self.run_query_pandas_dataframe(self.db_dialect.get_sql_list_objects())
        objects.columns = map(str.lower, objects.columns)
        return objects[objects.name.str.match(regex)]

    def get_objects(self):
        return self.run_query_pandas_dataframe(self.db_dialect.get_sql_list_objects())

    def get_columns(self, table_name):
        return self.run_query_pandas_dataframe(self.db_dialect.get_sql_list_columns(table_name))

    def get_objects_for_databases(self, databases):
        orig_ds = self.data_source
        ret = None
        for db in databases:
            try:
                print("Retrieving objects from: " + db)
                self.set_data_source(db)
                ret_tmp = self.run_query_pandas_dataframe(self.db_dialect.get_sql_list_objects())
                ret_tmp["DATABASE_NAME"] = db
                if ret is None:
                    ret = ret_tmp
                else:
                    ret = ret.append(ret_tmp)
            except Exception as e:

                print(e, file=stderr)
        self.set_data_source(orig_ds)
        return ret;

    def get_object_for_databases_single_query(self, databases):
        return self.run_query_pandas_dataframe(
            self.db_dialect.get_sql_list_objects_from_datbases_single_query(databases))

    def get_connector_for_each_database(self):
        ret={}
        for db in self.get_databases():

            new_conn = self.clone_set_data_source(db)
            new_conn.load_db_connectors=False
            ret[db] = new_conn
        return ret

    def load_db_sub_connectors(self):
        """
        Adds database connectors for each database
        :return:
        """
        for db, con in self.get_connector_for_each_database().items():
            self.DS._add_attr(db, con)

    @classmethod
    def df_to_excel(filename):
        spark_helper.df_to_excel(filename)

    @classmethod
    def df_to_csv(dir):
        spark_helper.df_to_csv(dir)

