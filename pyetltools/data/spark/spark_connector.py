import pyspark
import pandas
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.spark import spark_helper


class SparkConnector(Connector):

    @property
    def sql(self):
        if not self._sql:
            self._sql = self.get_spark_session().sql
        return self._sql

    def get_spark_session(self):
        if not self._spark_session:
            self._spark_session = get_spark_session(self.master, self.options)
        return self._spark_session

    def __init__(self, key, master, options=None):
        super().__init__(key=key)
        self.master=master
        self.options = options
        self._sql = None
        self._spark_session = None
        self.spark_helper = spark_helper

    def validate_config(self):
        super().validate_config()


    def get_df_from_jdbc(self, jdbc_conn_string, query_or_table, driver, username, get_pasword):
        cf = self.get_spark_session().read.format("jdbc") \
            .option("url", jdbc_conn_string) \
            .option("dbtable", "(" + query_or_table + ") x") \
            .option("driver", driver)
        if username:
            cf = cf.option("user", username)
        if get_pasword:
            password=get_pasword()
            if password:
                cf = cf.option("password", password)
        df = cf.load()

        df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
        return df



# Auxiliary functions
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

    # infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
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


def df_to_excel(filename):
    spark_helper.df_to_excel(filename)


def df_to_csv(dir):
    spark_helper.df_to_csv(dir)


def get_spark_session(master, spark_params={}):
    app_name = ""
    conf = SparkConf() \
        .setAppName(app_name) \
        .setMaster(master)

    for (key, value) in spark_params.items():
        conf = conf.set(key, value)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
