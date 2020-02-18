from pyetltools.data.db_dialect import DBDialect

class DB2DBDialect(DBDialect):
    def get_sql_list_objects(self):
        raise NotImplemented()

    def get_sql_list_databases(self):
        raise NotImplemented()

    def get_select(self, limit, table_name, where):
        return "select  * from {table_name} where {where} top {limit}"

    def get_jdbc_driver(self):
        return "com.ibm.db2.jcc.DB2Driver"

    def get_jdbc_subprotocol(self):
        return "db2"

    def get_odbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver, integrated_security):

        ret=""
        if dsn is not None:
            ret = ret+f"DSN={dsn};"
        if data_source is not None:
            ret = ret+f"DBALIAS={data_source};"
        if odbc_driver is not None:
            ret = ret + f"Driver={{{odbc_driver}}};"
        if integrated_security:
            ret = ret + f"Trusted_Connection=yes;"
        else:
            ret = ret + f"Pwd={password_callback()};"
            ret = ret + f"Uid={username};"
        return ret


    # constructs jdbc string: jdbc:sqlserver://pd0240\pdsql0240:1521;databaseName={database};integratedSecurity=true
    def get_jdbc_conn_string(self):
        ret = "jdbc:" + self.get_jdbc_subprotocol() + "://"
        ret = ret + self.host
        ret = ret+":" + str(self.port)
        if self.data_source is not None:
            ret = ret+f"/{self.data_source}"
        return ret

