from abc import abstractmethod
from copy import copy

import pyodbc

from pyetltools.core.connection import Connection

class DBConnection(Connection):
    def __init__(self, config):
        super().__init__(config)
        self.data_source= self.config.data_source

    @abstractmethod
    def get_jdbc_subprotocol(self):
        pass

    def set_data_source(self, data_source):
        self.data_source=data_source




    # constructs odbc connection string in the form DSN=NZFTST2;DATABASE={database};UID={username}
    def get_odbc_conn_string(self):
        ret=""
        if self.config.dsn is not None:
            ret = ret+f"DSN={self.config.dsn};"
        if self.config.host is not None:
            ret = ret+f"SERVER={self.config.host};"
        if self.config.port is not None:
            ret = ret+f"PORT={self.config.port};"
        if self.data_source is not None:
            ret = ret+f"DATABASE={self.data_source};"
        if self.config.dsn is not None:
            ret = ret+f"UID={self.config.username};"
        if self.config.odbc_driver is not None:
            ret = ret + f"Driver={{{self.config.odbc_driver}}};"
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



