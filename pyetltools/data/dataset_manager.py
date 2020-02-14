'''
Package preloads predefined datasets
'''

from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector



class Query:
    def __init__(self):
        pass


class Dataset():

    def __init__(self, key=None, db_connector_key=None, query=None, query_arguments=None, table=None, data_source=None,
                 spark_register_name=None, lazy_load=False):
        assert query is not None or table is not None, "DatasetConfig has to have one of the query and table parameters set. Both are None"
        super().__init__()
        self.key=key
        self.db_connector_key = db_connector_key
        self.query = query
        self.query_arguments = query_arguments
        self.table = table
        self.data_source = data_source
        self.spark_register_name = spark_register_name
        self.lazy_load = lazy_load

        self.df_spark = None
        self.df_pandas = None
        self.db_connector = connector.get(self.db_connector_key)
        assert  isinstance(self.db_connector, DBConnector), "Connector {self.config.db_connector} is not of the type DBConnector"

    def get_query(self):
        if self.query is not None:
            query = self.query
            if self.query_arguments is not None:
                return query.format_map(self.query_arguments)
        else:
            return f"select * from {self.table}"

    def get_key(self):
        if self.key is not None:
            key = self.key
        else:
            data_source_part = ""
            if self.data_source is not None:
                data_source_part = "_" + self.data_source
            data_connector_part = self.db_connector.replace('\\', '_')
            if self.table is not None:
                table_part = self.table
            else:
                table_part = hash(self.query)
            key = f"{data_connector_part}{data_source_part}_{table_part}"
        return key

    def load_spark_df(self):
        # gets spark data frame and discards result
        self.get_spark_df()

    def get_spark_df(self):
        if not self.df_spark:
            con: DBConnector = connector.get(self.db_connector_key)
            if self.data_source:
                con.set_data_source(self.data_source)
            df = con.run_query_spark_dataframe(self.get_query(),
                                               self.spark_register_name if self.spark_register_name
                                               else self.key)
            self.df_spark = df
        return self.df_spark

    def get_pandas_df(self):
        # TODO
        pass


class DatasetManager(Connector):

    def __init__(self, key, datasets, spark_connector=None):
        super().__init__(key)
        self.spark_connector = spark_connector
        self.datasets={}

        for ds in datasets:
            key = self.add_dataset(ds)

    def add_dataset(self, ds):
        key=ds.get_key()
        if key in self.datasets:
            raise Exception("Dataset with key {key} already exists.")
        self.datasets[key] = ds

        if not ds.lazy_load:
            # force dataset load
            ds.get_spark_df(self, key)

        if not hasattr(DatasetManager, key):
            setattr(DatasetManager, key, ds)
        return key
