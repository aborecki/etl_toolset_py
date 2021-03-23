import urllib
from abc import abstractmethod


class DBDialect:


    @abstractmethod
    def get_jdbc_subprotocol(self):
        pass

    @abstractmethod
    def get_jdbc_conn_string(self):
        pass


    @abstractmethod
    def get_jdbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver, integrated_security):
        pass

    # constructs odbc connection string in the form DSN=NZFTST2;DATABASE={database};UID={username}
    def get_odbc_conn_string(self, dsn, host, port, data_source, username, password_callback, odbc_driver, integrated_security, odbc_conn_options):
        ret = ""
        if dsn is not None:
            ret = ret + f"DSN={dsn};"
        if host is not None:
            ret = ret + f"SERVER={host};"
        if port is not None:
            ret = ret + f"PORT={port};"
        if data_source is not None:
            ret = ret + f"DATABASE={data_source};"
        if username is not None:
            ret = ret + f"UID={username};"
        if odbc_driver is not None:
            ret = ret + f"Driver={{{odbc_driver}}};"
        if integrated_security:
            ret = ret + f"Trusted_Connection=yes;"
        if odbc_conn_options is not None:
            ret = ret + odbc_conn_options

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
