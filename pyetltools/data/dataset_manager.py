'''
Package preloads predefined datasets
'''
from datetime import datetime
from pyspark.sql.session import SparkSession

from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector
from pyetltools.data.spark.spark_connector import SparkConnector


class Query:
    def __init__(self):
        pass


class Dataset():

    HIVE_SCHEMA="dataset_manager"
    HIVE_SCHEMA_ARCHIVE="dataset_manager_archive"

    def __init__(self, key=None, db_connector=None, query=None, query_arguments=None, table=None, data_source=None,
                 spark_register_name=None, lazy_load=False, cache_in_hive=False, spark_connector_key="SPARK"):
        assert query is not None or table is not None, "DatasetConfig has to have one of the query and table parameters set. Both are None"
        super().__init__()
        self.key = key
        self.db_connector_key = db_connector
        self.query = query
        self.query_arguments = query_arguments
        self.table = table
        self.data_source = data_source
        self.spark_register_name = spark_register_name
        self.lazy_load = lazy_load
        self.cache_in_hive = cache_in_hive
        self.df_spark = None
        self.df_pandas = None
        self.db_connector = connector.get(self.db_connector_key)
        self.spark_connector_key=spark_connector_key
        self.spark_connector: SparkConnector = connector.get(spark_connector_key)

        assert isinstance(self.db_connector,
                          DBConnector), "Connector {self.config.db_connector} is not of the type DBConnector"

        if not self.lazy_load:
            self.load_spark_df()

    def validate_config(self):
        connector.get(self.db_connector_key)
        connector.get(self.spark_connector_key)

    def check_if_schema_exists(self, schema):
        spark_session: SparkSession = self.spark_connector.get_spark_session();
        return len([db for db in spark_session.catalog.listDatabases()
                    if db.name.lower() == schema.lower()]) > 0

    def check_if_table_exists(self, table_name):
        spark_session: SparkSession = self.spark_connector.get_spark_session();
        cnt= len([db for db in spark_session.catalog.listTables(Dataset.HIVE_SCHEMA)
                    if db.name.lower() == table_name.lower()])
        return cnt>0

    def save_as_hive_table(self):
        spark_session: SparkSession = self.spark_connector.get_spark_session();
        if not self.check_if_schema_exists(Dataset.HIVE_SCHEMA):
            spark_session.sql("create schema "+Dataset.HIVE_SCHEMA)
        if not self.check_if_schema_exists(Dataset.HIVE_SCHEMA_ARCHIVE):
            spark_session.sql("create schema "+Dataset.HIVE_SCHEMA_ARCHIVE)

        if self.check_if_table_exists(self.key):
            dateTimeObj = datetime.now()
            timestampStr = dateTimeObj.strftime("%Y%m%d_%H%M%S")
            spark_session.sql(f"select * from {Dataset.HIVE_SCHEMA}."+ self.key).\
                write.saveAsTable(f"{Dataset.HIVE_SCHEMA_ARCHIVE}." + self.key+"_"+timestampStr)
            spark_session.sql(f"drop table {Dataset.HIVE_SCHEMA}.{self.key}")
        if self.df_spark is None:
            self.refresh_spark_df_from_source()
        self.df_spark.write.saveAsTable("dataset_manager." + self.key)

    def get_from_hive_spark_df(self):
        spark_session: SparkSession = self.spark_connector.get_spark_session()

        if self.check_if_table_exists(self.key):
            df = spark_session.sql(f"select * from  {Dataset.HIVE_SCHEMA}." + self.key)
            self.df_spark = df
            return self.df_spark
        else:
            return None

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

    def load_pandas_df(self):
        # gets pandas data frame and discards result
        self.get_pandas_df()

    def refresh_spark_df_from_source(self):
        con: DBConnector = connector.get(self.db_connector_key)
        if self.data_source:
            con.set_data_source(self.data_source)
        df = con.run_query_spark_dataframe(self.get_query(),
                                           self.spark_register_name if self.spark_register_name
                                           else self.key)
        self.df_spark = df

    def get_spark_df(self):
        data_source = "cache"
        if not self.df_spark:
            # not loaded yet
            if self.cache_in_hive:
                # maybe cached in hive? if not it will return None
                self.df_spark = self.get_from_hive_spark_df()
                data_source = "hive cache"
            if not self.df_spark:
                # still None - get data from database
                self.refresh_spark_df_from_source()
                if self.cache_in_hive:
                    self.save_as_hive_table()
                data_source = "database"
        print("Spark DF retrieved from " + data_source + ".")
        return self.df_spark

    def get_pandas_df(self):
        if self.df_pandas is None:
            con: DBConnector = connector.get(self.db_connector_key)
            if self.data_source:
                con.set_data_source(self.data_source)
            print("Starting query " + self.get_query(), end="")
            self.df_pandas = con.run_query_pandas_dataframe(self.get_query())
            print(" DONE")

        return self.df_pandas;


class DatasetManager(Connector):

    def __init__(self, key, datasets, spark_connector="SPARK"):
        super().__init__(key)
        self.spark_connector = spark_connector
        self.datasets = {}

        for ds in datasets:
            key = self.add_dataset(ds)

    def validate_config(self):
        super().validate_config()
        connector.get(self.spark_connector)

    def add_dataset(self, ds):
        key = ds.get_key()
        if key in self.datasets:
            raise Exception("Dataset with key {key} already exists.")
        self.datasets[key] = ds

        if not ds.lazy_load:
            # force dataset load
            ds.get_spark_df(self, key)

        if not hasattr(DatasetManager, key):
            setattr(DatasetManager, key, ds)
        return key
