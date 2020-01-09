from abc import ABC

from pyetltools.data.core.db_connection import DBConnection


class SQLServerDBConnection(DBConnection):

    def get_sql_list_objects(self):
        return """select TABLE_SCHEMA [SCHEMA], table_name as NAME, TABLE_TYPE as
         TYPE from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')"""

    def get_sql_list_databases(self):
        return "SELECT NAME FROM sys.databases"

    def get_select(self, limit_rows, table_name, where):
        return "select limit {limit} * from {table_name} where {where}"
