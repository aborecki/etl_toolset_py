'''
Package preloads predefined datasets
'''
from datetime import datetime
from pyspark.sql.session import SparkSession

from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector
from pyetltools.data.spark.spark_connector import SparkConnector

import os
class Query:
    def __init__(self):
        pass




def set_default_cache_folder(folder):
    global DEFAULT_CACHE_FOLDER
    DEFAULT_CACHE_FOLDER=folder

def get_default_cache_folder():
    return DEFAULT_CACHE_FOLDER


class FilesystemCache:
    def __init__(self, dataset, folder, spark_connector="SPARK"):
        self.folder=folder
        self.dataset = dataset
        self.spark_connector=spark_connector


    def get_spark_connector(self):
        return connector.get(self.spark_connector)

    def get_file_name_name(self):
        if not self.dataset.key:
            raise Exception("Cannot cache in file - no key set for dataset")
        return   os.path.join(self.folder, self.dataset.key.replace("/","_")+".parquet")

    def get_spark_df(self):
        try:
            df=self.get_spark_connector().get_spark_session().read.parquet(self.get_file_name_name())
            return df
        except:
            return None

    def save(self):
        if self.dataset.df_spark is None:
            self.dataset.load_spark_df()
        file_path=self.get_file_name_name()
        if os.path.exists(file_path):
            import shutil
            shutil.rmtree(file_path)
        self.dataset.df_spark.write.parquet(file_path)
        print("Dataset saved as " + file_path)

class HiveCache:
    HIVE_SCHEMA="datasets"
    HIVE_SCHEMA_ARCHIVE="datasets_archive"

    def __init__(self, dataset, spark_connector="SPARK", hive_schema=None, hive_archive_schema=None, hive_archive_before_drop=True):
        self.hive_schema = hive_schema if hive_schema else HiveCache.HIVE_SCHEMA
        self.hive_archive_schema = hive_archive_schema if hive_archive_schema else HiveCache.HIVE_SCHEMA_ARCHIVE
        self.hive_archive_before_drop=hive_archive_before_drop
        self.spark_connector = spark_connector
        self.dataset=dataset

    def get_spark_connector(self):
        return connector.get(self.spark_connector)

    def get_spark_session(self):
        return self.get_spark_connector().get_spark_session()

    def check_if_hive_schema_exists(self, schema):
        spark_session: SparkSession = self.get_spark_session()
        return len([db for db in spark_session.catalog.listDatabases()
                    if db.name.lower() == schema.lower()]) > 0

    def check_if_hive_table_exists(self, table_name):
        spark_session: SparkSession = self.get_spark_session()

        if not self.check_if_hive_schema_exists(self.hive_schema):
            return False

        listOfTables = spark_session.catalog.listTables(self.hive_schema) if self.hive_schema \
            else spark_session.catalog.listTables()
        cnt= len([tb for tb in listOfTables
                    if tb.name.lower() == table_name.lower()  and not tb.isTemporary#
                  ])
        return cnt>0

    def get_hive_table_name(self):
        return  self.dataset.key.replace("/","_")

    def t(self):
        return self.get_hive_table_name_with_schema()

    def get_hive_table_name_with_schema(self):
        return (self.hive_schema + '.' if self.hive_schema else '') + self.get_hive_table_name()

    def get_hive_archive_table_name_with_schema(self):
        return (self.hive_archive_schema + '.' if self.hive_archive_schema else '') + self.get_hive_table_name()

    def save(self):
        spark_session: SparkSession = self.get_spark_session()
        if not self.check_if_hive_schema_exists(self.hive_schema):
            spark_session.sql("create schema "+self.hive_schema)
        if not self.check_if_hive_schema_exists(self.hive_archive_schema):
            spark_session.sql("create schema "+self.hive_archive_schema)
        table_name=self.get_hive_table_name_with_schema()
        archive_table_name = self.get_hive_archive_table_name_with_schema()

        if self.check_if_hive_table_exists(self.get_hive_table_name()):
            if self.hive_archive_before_drop:
                dateTimeObj = datetime.now()
                timestampStr = dateTimeObj.strftime("%Y%m%d_%H%M%S")
                arch_table_name=f"{archive_table_name}_"+timestampStr
                print(f"Archiving table as {arch_table_name}")
                spark_session.sql(f"select * from {table_name}").\
                    write.saveAsTable(arch_table_name)
            print(f"Dropping table {arch_table_name}.")
            spark_session.sql(f"drop table {table_name}")
        if self.dataset.df_spark is None:
            self.dataset.load_spark_df()
        self.dataset.df_spark.write.saveAsTable(table_name)

    def get_spark_df(self):
        spark_session: SparkSession = self.get_spark_connector().get_spark_session()
        if self.check_if_hive_table_exists(self.get_hive_table_name()):
            df = spark_session.sql(f"select * from  {self.get_hive_table_name_with_schema()}")
            print("Hive snapshot created time:" +spark_session.sql("desc formatted  "+ self.get_hive_table_name_with_schema()).filter(
                'col_name="Created Time"').select(["data_type"]).head().data_type)
            self.df_spark = df
            return self.df_spark
        else:
            return None




class Dataset(Connector):
    def __init__(self, key=None, db_connector=None, query=None, query_arguments=None, table=None, data_source=None,
                 spark_register_name=None, lazy_load=True, spark_connector="SPARK",
                 cache_in_hive=False, cache_in_filesystem=False, hive_schema=None, hive_archive_schema=None,
                 hive_archive_before_drop=True, cache_folder=None
                 ):
        assert query is not None or table is not None, "DatasetConfig has to have one of the query and table parameters set. Both are None"
        super().__init__(key)
        self.key = key
        self.db_connector = db_connector
        self.query = query
        self.query_arguments = query_arguments
        self.table = table
        self.data_source = data_source
        self.spark_register_name = spark_register_name
        self.lazy_load = lazy_load
        self.cache_in_hive = cache_in_hive
        self.cache_in_filesystem=cache_in_filesystem
        self.hive_cache=None
        self.cache_folder=cache_folder

        if cache_in_hive:
            self.hive_cache=HiveCache(self, spark_connector,
                     hive_schema, hive_archive_schema,
                     hive_archive_before_drop)

        if cache_in_filesystem:
            if not self.cache_folder:
                self.cache_folder=get_default_cache_folder()
            self.filesystem_cache=FilesystemCache(self, self.cache_folder)

        self.df_spark = None
        self.df_pandas = None
        self.spark_connector = spark_connector
        self._db_connector=None

        if not self.lazy_load:
            print(f"Pre-loading dataset {self.key} as lazy_load parameter set to False")
            self.load_spark_df()

    def get_db_connector(self):
        if not self._db_connector:
            self._db_connector=connector.get(self.db_connector)
            if self.data_source:
                self._db_connector = self._db_connector.with_data_source(self.data_source)
        return self._db_connector

    def get_spark_connector(self):
        return connector.get(self.spark_connector)

    def get_spark_session(self):
        return self.get_spark_connector().get_spark_session()

    def validate_config(self):
        db_connector=self.get_db_connector()
        spark_connector=self.get_spark_connector()

        assert isinstance(db_connector,
                          DBConnector), "Connector {self.db_connector} is not of the type DBConnector"
        assert isinstance(spark_connector,
                          SparkConnector), "Connector {self.spark_connector} is not of the type SparkConnector"



    def get_query_with_args(self, args=None):
        if self.query is not None:
            query = self.query
            if args:
                return query.format_map(args)
            else:
                if self.query_arguments:
                    return query.format_map(self.query_arguments)
                else:
                    return query
        else:
            return f"select * from {self.table}"

    def get_query_arguments(self):
        return self.query_arguments

    def set_query_arguments(self, args):
        self.query_arguments=args

    def get_query(self, args=None):
        arguments=self.query_arguments
        if args:
            arguments=self.query_arguments
        return self.get_query_with_args(arguments)

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

    def get_spark_register_name(self):
        if self.spark_register_name :
            return self.spark_register_name
        else:
            if self.key:
                return self.key.replace("/","_")

    def refresh_spark_df_from_source(self, args=None):
        con: DBConnector = self.get_db_connector()

        df = con.run_query_spark_dataframe(self.get_query(args), self.get_spark_register_name()
                                         )
        self.df_spark = df
        if self.cache_in_hive:
            self.hive_cache.save()
        if self.cache_in_filesystem:
            self.filesystem_cache.save()

    def run_custom_query_spark_df(self, custom_query, args=None, spark_register_name=None):
        acct_args={};
        if self.query_arguments:
            acct_args=self.query_arguments
        if args:
            acct_args.update(args)
        acct_args.update({"query":self.get_query_with_args(acct_args)})

        return self.get_db_connector().run_query_spark_dataframe(
            custom_query.format_map(acct_args),
            spark_register_name)

    def run_query_spark_df(self, args=None, spark_register_name=None):
        return self.get_db_connector().run_query_spark_dataframe(self.get_query_with_args(args),
                                           spark_register_name)


    def get_spark_df(self):

        data_source = "cache"
        if not self.df_spark:
            # not loaded yet

            if self.cache_in_filesystem:
                print("Trying to retrieve from filesystem")
                self.df_spark = self.filesystem_cache.get_spark_df()
                data_source = "filesystem cache"
                if not self.df_spark:
                    print("Filesystem retrieval unsuccessful")
            if not self.df_spark and self.cache_in_hive:
                # maybe cached in hive? if not it will return None
                print("Trying to retrieve from hive")
                self.df_spark = self.hive_cache.get_spark_df()
                data_source = "hive cache"
                if not self.df_spark:
                    print("Hive retrieval unsuccessful")
            if not self.df_spark:
                # still None - get data from database
                self.refresh_spark_df_from_source()
                if self.cache_in_hive:
                    self.hive_cache.save()
                if self.cache_in_filesystem:
                    self.filesystem_cache.save()
                data_source = "database"
        spark_register_name=self.get_spark_register_name()
        if spark_register_name:
            print("Registering temp table as: "+spark_register_name)
            self.df_spark.registerTempTable(spark_register_name)
        print("Spark dataframe retrieved from " + data_source + ".")
        return self.df_spark

    def get_pandas_df(self):
        if self.df_pandas is None:
            con: DBConnector = self.get_db_connector()
            if self.data_source:
                con.with_data_source(self.data_source)
            print("Starting query " + self.get_query(), end="")
            self.df_pandas = con.run_query_pandas_dataframe(self.get_query())
            print(" DONE")

        return self.df_pandas

