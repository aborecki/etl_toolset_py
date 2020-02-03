from abc import ABC, abstractmethod

from pyetltools.data.core.connection import DBConnection


class SQLServerDBConnection(DBConnection):

    def get_sql_list_objects(self):
        return """select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')"""

    def get_sql_list_objects_from_datbases_single_query(self, databases):
        return " UNION ALL \n".join(
            [f"""select '{db}' as DATABASE_NAME, * from {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')""" for db in  databases])

    def get_sql_list_databases(self):
        return "SELECT name FROM sys.databases"

    def get_select(self, limit_rows, table_name, where):
        return "select limit {limit} * from {table_name} where {where}"

    def get_jdbc_subprotocol(self):
        return "sqlserver"

    def get_jdbc_driver(self):
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    # constructs jdbc string: jdbc:sqlserver://pd0240\pdsql0240:1521;databaseName={database};integratedSecurity=true
    def get_jdbc_conn_string(self):
        ret = "jdbc:" + self.get_jdbc_subprotocol() + "://"
        ret = ret + self.config.host
        ret = ret+":" + str(self.config.port)+";"

        if self.config.data_source is not None:
            ret = ret+f"databaseName={self.data_source};"
        if self.config.integrated_security:
            ret = ret + f"integratedSecurity=true;"
        else:
            if self.config.data_source is not None:
                ret = ret+f"DATABASE={self.config.data_source};"
        if self.config.dsn is not None:
            ret = ret+f"UID={self.config.username};"
        return ret

    @abstractmethod
    def supports_jdbc(self):
        return True;

    @abstractmethod
    def supports_odbc(self):
        return True;