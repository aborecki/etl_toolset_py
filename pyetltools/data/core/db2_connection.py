from abc import ABC, abstractmethod

from pyetltools.core import connection
from pyetltools.data.core.connection import DBConnection



class DB2DBConnection(DBConnection):

    def get_sql_list_objects(self):
        return """select  SCHEMA , tablename as NAME, 'TABLE' TYPE from _v_table UNION ALL 
              select  SCHEMA,  viewname as NAME, 'VIEW'  as TYPE from _v_view UNION ALL 
              select  SCHEMA,  synonym_name as NAME, 'SYNONYM'  as TYPE from _v_synonym """

    def get_sql_list_databases(self):
        return "select database as NAME from _v_database"

    def get_select(self, limit, table_name, where):
        return "select * from {table_name} where {where} top {limit}"

    def get_jdbc_driver(self):
        return "com.ibm.db2.jcc.DB2Driver"

    def get_jdbc_subprotocol(self):
        return "db2"


    def get_odbc_conn_string(self):
        ret=""
        if self.config.dsn is not None:
            ret = ret+f"DSN={self.config.dsn};"
        if self.data_source is not None:
            ret = ret+f"DBALIAS={self.data_source};"
        if self.config.odbc_driver is not None:
            ret = ret + f"Driver={{{self.config.odbc_driver}}};"
        if self.config.integrated_security:
            ret = ret + f"Trusted_Connection=yes;"
        else:
            ret = ret + f"Pwd={self.get_password()};"
            ret = ret + f"Uid={self.config.username};"
        return ret


    # constructs jdbc string: jdbc:sqlserver://pd0240\pdsql0240:1521;databaseName={database};integratedSecurity=true
    def get_jdbc_conn_string(self):
        ret = "jdbc:" + self.get_jdbc_subprotocol() + "://"
        ret = ret + self.config.host
        ret = ret+":" + str(self.config.port)
        if self.data_source is not None:
            ret = ret+f"/{self.data_source}"
        return ret

    @abstractmethod
    def supports_jdbc(self):
        return True;

    @abstractmethod
    def supports_odbc(self):
        return True;