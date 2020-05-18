import copy
from abc import abstractmethod

import pyodbc
import pandas

from pyetltools.core import connector
from pyetltools.core.attr_dict import AttrDict
from pyetltools.core.connector import Connector
from pyetltools.data.spark import spark_helper
from sys import stderr
import string




class DBConnector(Connector):
    def __init__(self,
                 key=None,
                 jdbc_conn_string=None,
                 odbc_conn_string=None,
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
                 spark_connector="SPARK",
                 db_dialect_class=None
                 ):
        super().__init__(key=key)
        assert key, "Config key cannot be None"
        self.jdbc_conn_string = jdbc_conn_string
        self.odbc_conn_string = odbc_conn_string
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
        self.spark_connector = spark_connector
        self.db_dialect_class = db_dialect_class

        self.db_dialect=self.db_dialect_class()
        self.DS=AttrDict(initializer=lambda _: self.load_db_sub_connectors() )
        self._odbcconnection=None
        from pyetltools.data.sql_alchemy_connector import SqlAlchemyConnector

        self.sql_alchemy_connector= SqlAlchemyConnector(self)

        if self.load_db_connectors:
            self.load_db_sub_connectors()

    def validate_config(self):
        super().validate_config()
        connector.get(self.spark_connector)

    def get_spark_connector(self):
        return connector.get(self.spark_connector)

    def with_data_source(self, data_source):
        cp=self.clone()
        cp.data_source=data_source
        return cp

    def clone(self):
        return DBConnector(key=self.key,
                 jdbc_conn_string=self.jdbc_conn_string,
                 odbc_conn_string=self.odbc_conn_string,
                 host=self.host,
                 port=self.port,
                 dsn=self.dsn,
                 username=self.username,
                 password=self.password,
                 data_source=self.data_source,
                 odbc_driver=self.odbc_driver,
                 integrated_security=self.integrated_security,
                 supports_jdbc=self.supports_jdbc,
                 supports_odbc=self.supports_odbc,
                 load_db_connectors=self.load_db_connectors,
                 spark_connector=self.spark_connector,
                 db_dialect_class=self.db_dialect_class)

    def get_password(self):
        if self.integrated_security:
            return None
        else:
            return super().get_password()

    def get_sqlalchemy_conn_string(self):
        return self.db_dialect.get_sqlalchemy_conn_string(self.get_odbc_conn_string(), self.get_jdbc_conn_string())


    def get_odbc_conn_string(self):
        if self.odbc_conn_string:
            return self.odbc_conn_string
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
        if self.jdbc_conn_string:
            return self.jdbc_conn_string
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
         #if not self._odbcconnection:
        return   pyodbc.connect(self.get_odbc_conn_string())
        #return self._odbcconnection;

    def run_query_spark_dataframe(self, query,  registerTempTableName=None):
        if self.supports_jdbc:
            print("Executing query (JDBC):"+query)
            ret=self.get_spark_connector().get_df_from_jdbc(self.get_jdbc_conn_string(),
                                                      query,
                                                      self.db_dialect.get_jdbc_driver(),
                                                      self.username,
                                                      self.get_password)
        else:
            print("Warning: conversion from panadas df to spark needed. Can be slow.")
            print("Executing query (ODBC):" + query)
            pdf = self.run_query_pandas_dataframe(query)
            is_empty = pdf.empty
            ret=None
            if not is_empty:
                ret = spark_helper.pandas_to_spark(self.get_spark_connector().get_spark_session(), pdf)
            else:
                print("No data returned.")
        if registerTempTableName is not None:
            ret.registerTempTable(registerTempTableName)
        return ret

    def run_query_pandas_dataframe(self, query):
        conn = pyodbc.connect(self.get_odbc_conn_string())
        return pandas.read_sql(query, conn, coerce_float=False, parse_dates=None)

    def execute_statement(self, statement):
        conn = self.get_odbc_connection()


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
        ret = None
        for db in databases:
            try:
                print("Retrieving objects from: " + db)
                ds=self.with_data_source(db)
                ret_tmp = ds.run_query_pandas_dataframe(self.db_dialect.get_sql_list_objects())
                ret_tmp["DATABASE_NAME"] = db
                if ret is None:
                    ret = ret_tmp
                else:
                    ret = ret.append(ret_tmp)
            except Exception as e:

                print(e, file=stderr)
        return ret;

    def get_object_for_databases_single_query(self, databases):
        return self.run_query_pandas_dataframe(
            self.db_dialect.get_sql_list_objects_from_datbases_single_query(databases))

    def get_connector_for_each_database(self):
        ret={}
        for db in self.get_databases():

            new_conn = self.with_data_source(db)
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

