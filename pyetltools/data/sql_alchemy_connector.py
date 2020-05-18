import sqlalchemy as sa
import urllib.parse

from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector
import pyetltools.data.sqlalchemy.netezza_dialect



class SqlAlchemyConnector(Connector):
    def __init__(self, db_connector:DBConnector):
        super().__init__("")
        self.db_connector=db_connector

    def validate_config(self):
        return True


    def get_engine(self):
        con_str = self.db_connector.get_sqlalchemy_conn_string()
        return sa.create_engine(con_str)


    def connect(self):
        return self.get_engine().connect();


    def get_session_class(self):
        return sessionmaker(bind=self.get_engine())


    def get_session(self):
        # create object of the sqlalchemy class
        return self.get_session_class()()


    def get_metadata_for_table(self, table):
        from sqlalchemy import MetaData
        metadata = MetaData(bind=self.get_engine(), reflect=False)
        schema_table_split = table.split(".")
        # take last part as tablename and part before as schema
        schema = None
        _table = table
        _schema=None
        if len(schema_table_split) > 1:
            _table = schema_table_split[-1]
            _schema = schema_table_split[-2]

        metadata.reflect(only=[_table], schema=_schema)
        return metadata.tables[table]

    def get_inspector(self):
        return inspect(self.get_engine())


    def helper_print_object_template(self, table):
        table_meta_data = self.get_metadata_for_table(table)
        ret = []
        for i in table_meta_data.columns:
            x = repr(i)
            ss = x.split(",")
            ret.append(i.name.lower() + "= Column(" + ss[1] + ")")
        print("\n".join(ret))

        ret = []
        for i in table_meta_data.columns:
            ret.append("'" + i.name.lower() + "'")
        print("__ordering=[" + ",".join(ret) + "]")
