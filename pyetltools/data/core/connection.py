from abc import abstractmethod

import pyodbc

from pyetltools.core.connection import Connection

class DBConnection(Connection):
    def __init__(self, config):
        super().__init__(config)

    @abstractmethod
    def get_jdbc_subprotocol(self):
        pass

    # constructs jdbc string: jdbc:sqlserver://pd0240\pdsql0240:1521;databaseName={database};integratedSecurity=true
    def get_jdbc_conn_string(self):
        ret = "jdbc:" + self.get_jdbc_subprotocol() + "://"
        ret = ret + self.config.host
        ret = ret+":" + str(self.config.port)+";"

        if self.config.data_source is not None:
            ret = ret+f"databaseName={self.config.data_source};"
        if self.config.integrated_security:
            ret = ret + f"integratedSecurity=true;"
        else:
            if self.config.data_source is not None:
                ret = ret+f"DATABASE={self.config.data_source};"
        if self.config.dsn is not None:
            ret = ret+f"UID={self.config.username};"
        return ret

    # constructs odbc connection string in the form DSN=NZFTST2;DATABASE={database};UID={username}
    def get_odbc_conn_string(self):
        ret=""
        if self.config.dsn is not None:
            ret = ret+f"DSN={self.config.dsn};"
        if self.config.host is not None:
            ret = ret+f"SERVER={self.config.host};"
        if self.config.data_source is not None:
            ret = ret+f"DATABASE={self.config.data_source};"
        if self.config.dsn is not None:
            ret = ret+f"UID={self.config.username};"
        if self.config.driver is not None:
            ret = ret + f"Driver={{{self.config.driver}}};"
        if self.config.integrated_security:
            ret = ret + f"Trusted_Connection=yes;"
        return ret

    @abstractmethod
    def get_sql_list_objects(self):
        pass

    @abstractmethod
    def get_sql_list_databases(self):
        pass

    @abstractmethod
    def get_select(self, limit_rows, table_name, where):
        pass

    def get_odbc_connection(self):
        pyodbc.connect(self.get_odbc_conn_string())

    @abstractmethod
    def supports_jdbc(self):
        pass;

    @abstractmethod
    def supports_odbc(self):
        pass;



