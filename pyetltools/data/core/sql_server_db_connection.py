from abc import ABC, abstractmethod

from pyetltools.data.core.connection import DBConnection


class SQLServerDBConnection(DBConnection):

    def get_sql_list_objects(self):
        return """select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')"""

    def get_sql_list_databases(self):
        return "SELECT * FROM sys.databases"

    def get_select(self, limit_rows, table_name, where):
        return "select limit {limit} * from {table_name} where {where}"

    def get_jdbc_subprotocol(self):
        return "sqlserver"

    @abstractmethod
    def supports_jdbc(self):
        return True;

    @abstractmethod
    def supports_odbc(self):
        return True;