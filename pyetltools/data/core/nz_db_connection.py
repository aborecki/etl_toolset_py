from abc import ABC, abstractmethod

from pyetltools.core import connection
from pyetltools.data.core.connection import DBConnection



class NZDBConnection(DBConnection):

    def get_sql_list_objects(self):
        return """select  SCHEMA , tablename as NAME, 'TABLE' TYPE from _v_table UNION ALL 
              select  SCHEMA,  viewname as NAME, 'VIEW'  as TYPE from _v_view UNION ALL 
              select  SCHEMA,  synonym_name as NAME, 'SYNONYM'  as TYPE from _v_synonym """

    def get_sql_list_databases(self):
        return "select database as NAME from _v_database"

    def get_select(self, limit, table_name, where):
        return "select * from {table_name} where {where} top {limit}"

    @abstractmethod
    def supports_jdbc(self):
        return False;

    @abstractmethod
    def supports_odbc(self):
        return True;