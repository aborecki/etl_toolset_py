import urllib
from abc import abstractmethod


class DBDialect:


    @abstractmethod
    def get_jdbc_subprotocol(self):
        raise NotImplemented()

    @abstractmethod
    def get_jdbc_conn_string(self):
        raise NotImplemented()

    def get_sqlalchemy_conn_string(self):
        return '{}+pyodbc:///?odbc_connect={}'.format(self.get_sqlalchemy_dialect(),
                                                      urllib.parse.quote_plus(self.get_odbc_conn_string()))

    @abstractmethod
    def get_jdbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver, integrated_security):
        raise NotImplemented()

    # constructs odbc connection string in the form DSN=NZFTST2;DATABASE={database};UID={username}
    def get_odbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver, integrated_security):
        ret = ""
        if dsn is not None:
            ret = ret + f"DSN={dsn};"
        if host is not None:
            ret = ret + f"SERVER={host};"
        if port is not None:
            ret = ret + f"PORT={port};"
        if data_source is not None:
            ret = ret + f"DATABASE={data_source};"
        if dsn is not None:
            ret = ret + f"UID={username};"
        if odbc_driver is not None:
            ret = ret + f"Driver={{{odbc_driver}}};"
        if integrated_security:
            ret = ret + f"Trusted_Connection=yes;"
        return ret

    @abstractmethod
    def get_sql_list_objects(self):
        raise NotImplemented()

    @abstractmethod
    def get_sql_list_databases(self):
        raise NotImplemented()

    @abstractmethod
    def get_select(self, limit_rows, table_name, where):
        raise NotImplemented()
