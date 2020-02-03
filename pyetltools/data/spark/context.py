import pyodbc
import pyspark
import pandas
from pyspark.sql.types import *

from pyetltools.core import context
from pyetltools.data.spark import spark_helper
from pyetltools.data.spark.config import SparkConfig
from pyetltools.data.spark.connection import SparkConnection


def df_to_excel(filename):
    spark_helper.df_to_excel(filename)


def df_to_csv(dir):
    spark_helper.df_to_csv(dir)


class SparkContext:
    def __init__(self, config: SparkConfig, connection: SparkConnection):
        self.config = config
        self.connection = connection
        self.sql=self.get_spark_session().sql

    @classmethod
    def create_connection_from_config(cls, config: SparkConfig):
        return SparkConnection(config)

    @classmethod
    def create_from_config(cls, config: SparkConfig):
        return SparkContext(config, cls.create_connection_from_config(config))

    def get_spark_session(self):
        return self.connection.get_spark_session()

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(SparkConfig, SparkContext)

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]':
        return DateType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except:
        typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(spark, pandas_df):
    columns = list(pandas_df.columns)

    struct_list = []

    #infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
    # pandas_df.apply(infer_type, axis=0)

    df_types = list(pandas.DataFrame(pandas_df.apply(pandas.api.types.infer_dtype, axis=0)).reset_index().rename(
        columns={'index': 'column', 0: 'type'}));
    types = list(pandas_df.dtypes)
    # pandas_df=pandas_df.astype(df_types)
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)

    try:
        return spark.createDataFrame(pandas_df).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    except:
        return spark.createDataFrame(pandas_df, p_schema).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)


