import urllib

from pyetltools.data.db_dialect import DBDialect

class NZDBDialect(DBDialect):

    def get_sql_list_objects(self):
        return """select  SCHEMA , tablename as NAME, 'TABLE' TYPE from _v_table UNION ALL 
              select  SCHEMA,  viewname as NAME, 'VIEW'  as TYPE from _v_view UNION ALL 
              select  SCHEMA,  synonym_name as NAME, 'SYNONYM'  as TYPE from _v_synonym """

    def get_sql_list_columns(self, table_name):
        return f"""
            select * from _v_sys_columns where table_name='{table_name}'
            order by ordinal_position
        """

    def get_sql_list_databases(self):
        return "select database as NAME from _v_database"

    def get_select(self, limit, table_name, where):
        return "select * from {table_name} where {where} top {limit}"

    def get_sqlalchemy_dialect(self):
        return "netezza"

    def get_sqlalchemy_driver(self):
        return "pyodbc"

    def get_sqlalchemy_conn_string(self, odbc_conn_string, jdbc_conn_string):
        return '{}+{}:///?odbc_connect={}'.format(self.get_sqlalchemy_dialect(),
                                                      self.get_sqlalchemy_driver(),
                                                      urllib.parse.quote_plus( odbc_conn_string))