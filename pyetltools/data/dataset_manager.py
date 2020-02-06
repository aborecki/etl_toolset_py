'''
Package preloads predefined datasets
'''
from pyetltools.config.config import Config
from pyetltools.core import context
from pyetltools.core.context import Context
from pyetltools.data.core.context import DBContext


class DatasetConfig(Config):
    def __init__(self, key=None, db_context=None, query=None, table=None, data_source=None, spark_register_name=None, lazy_load=False):
        assert query is not None or table is not None, "DatasetConfig has to have one of the query and table parameters set. Both are None"
        super().__init__(key)
        self.db_context = db_context
        self.query = query
        self.table = table

        self.data_source = data_source
        self.spark_register_name = spark_register_name
        self.lazy_load = lazy_load


class DatasetManagerConfig(Config):
    def __init__(self, key, datasets, spark_context=None):
        super().__init__(key)
        self.datasets = datasets
        self.spark_context=spark_context


class DatasetManagerContext(Context):

    def __init__(self, config: DatasetManagerConfig):

        self.ds = dict()

        for ds in config.datasets:
            key = self.add_dataset_from_config(ds)
            print("Loaded data set:" + str(key))

    def add_dataset_from_config(self, config):

        if config.key is not None:
            key=config.key
        else:
            data_source_part = ""
            if config.data_source is not None:
                data_source_part = "_" + config.data_source
            data_context_part = config.db_context.replace('\\', '_')
            if config.table is not None:
                table_part=config.table
            else:
                table_part=hash(config.query)
            key = f"{data_context_part}{data_source_part}_{table_part}"

        def get_dataset(self, key):
            if key in self.ds:
                return self.ds[key]
            else:
                print("Loading dataset:"+str(key))
                ctx: DBContext = context.get(config.db_context)

                if config.data_source is not None:
                    ctx.set_data_source(config.data_source)

                if config.query is not None:
                    spark_df = ctx.run_query_spark_dataframe(config.query, config.spark_register_name)
                else:
                    spark_df = ctx.run_query_spark_dataframe(f"select * from {config.table}",
                                                             config.spark_register_name)
                self.ds[key] = spark_df
                return spark_df

        if not hasattr(DatasetManagerContext, key):
            setattr(DatasetManagerContext, key, property(lambda self: get_dataset(self, key)))
            if not config.lazy_load:
                #setup dataset
                get_dataset(self, key)
        return key

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(DatasetManagerConfig, DatasetManagerContext)

    @classmethod
    def create_from_config(cls, config: DatasetManagerConfig):
        return DatasetManagerContext(config)
