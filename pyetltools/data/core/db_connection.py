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
        ret = ret + self.db_config.host
        ret = ":"+ret + self.db_config.port

        if self.db_config.data_source is not None:
            ret = ret+f"databaseName={self.db_config.data_source};"
        if self.db_config.integrated_security:
            ret = ret + f"integratedSecurity=yes;"
        else:
            if self.db_config.data_source is not None:
                ret = ret+f"DATABASE={self.db_config.data_source};"
        if self.db_config.dsn is not None:
            ret = ret+f"UID={self.db_config.username};"
        return ret

    # constructs odbc connection string in the form DSN=NZFTST2;DATABASE={database};UID={username}
    def get_odbc_conn_string(self):
        ret=""
        if self.db_config.dsn is not None:
            ret = ret+f"DSN={self.db_config.dsn};"
        if self.db_config.data_source is not None:
            ret = ret+f"DATABASE={self.db_config.data_source};"
        if self.db_config.dsn is not None:
            ret = ret+f"UID={self.db_config.username};"
        if self.db_config.driver is not None:
            ret = ret + f"Driver={{{self.db_config.driver}}};"
        if self.db_config.integrated_security:
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





