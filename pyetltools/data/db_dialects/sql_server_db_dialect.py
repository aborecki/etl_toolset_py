
from pyetltools.data.db_dialect import DBDialect

class SQLServerDBDialect(DBDialect):

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

    def get_sqlalchemy_dialect(self):
        return "mssql"

    def get_jdbc_driver(self):
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    # constructs jdbc string: jdbc:sqlserver://pd0240\pdsql0240:1521;databaseName={database};integratedSecurity=true
    def get_jdbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver, integrated_security):

        ret = "jdbc:" + self.get_jdbc_subprotocol() + "://"
        ret = ret + host
        ret = ret+":" + str(port)+";"

        if data_source is not None:
            ret = ret+f"databaseName={data_source};"
        if integrated_security:
            ret = ret + f"integratedSecurity=true;"
        #else:
        #    if data_source is not None:
        #        ret = ret+f"DATABASE={data_source};"
        if dsn is not None:
            ret = ret+f"UID={username};"
        return ret
