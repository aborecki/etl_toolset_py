import urllib

from pyetltools.data.db_dialect import DBDialect


class DB2DBDialect(DBDialect):
    def get_sql_list_objects(self):
        return "select * from SYSIBM.SYSTABLES"

    def get_sql_list_databases(self):
        raise NotImplemented()

    def get_select(self, limit, table_name, where):
        return "select  * from {table_name} where {where} top {limit}"

    def get_jdbc_driver(self):
        return "com.ibm.db2.jcc.DB2Driver"

    def get_jdbc_subprotocol(self):
        return "db2"

    def get_sqlalchemy_dialect(self):
        return "db2"

    def get_sqlalchemy_driver(self):
        return "pyodbc"

    #def get_sqlalchemy_conn_string(self, odbc_conn_string, jdbc_conn_string):
    #    return '{}+{}://{}'.format(self.get_sqlalchemy_dialect(),
    #                                              self.get_sqlalchemy_driver(),
    #                                              jdbc_conn_string[jdbc_conn_string.find("//")+2:])

    def get_sqlalchemy_conn_string(self, odbc_conn_string, jdbc_conn_string):
        return '{}+{}:///?odbc_connect={}'.format(self.get_sqlalchemy_dialect(),
                                                      self.get_sqlalchemy_driver(),
                                                      urllib.parse.quote_plus( odbc_conn_string))

    def get_odbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver,
                             integrated_security):
        ret = ""
        if dsn is not None:
            ret = ret + f"DSN={dsn};"
        if data_source is not None:
            ret = ret + f"DBALIAS={data_source};"
        if odbc_driver is not None:
            ret = ret + f"Driver={{{odbc_driver}}};"
        if integrated_security:
            ret = ret + f"Trusted_Connection=yes;"
        else:
            ret = ret + f"Pwd={password_callback()};"
            ret = ret + f"Uid={username};"
        return ret

    def get_jdbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver,
                             integrated_security):
        # jdbc:db2://DB2Connect.bec.dk:50000/CD99
        ret = "jdbc:" + self.get_jdbc_subprotocol() + "://"
        ret = ret + host
        ret = ret + ":" + str(port)

        if data_source is not None:
            ret = ret + f"/{data_source}"
        return ret
