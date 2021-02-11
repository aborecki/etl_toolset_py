import os
import tempfile

import pyspark
import pandas
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyetltools import logger
from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.spark import tools as spark_tools


class SparkConnector(Connector):

    @property
    def sql(self):
        if not self._sql:
            self._sql = self.get_spark_session().sql
        return self._sql

    def sql_query(self, query, register_temp_table=None, persist=True):
        df=self.sql(query)
        if register_temp_table is not None:
            df.registerTempTable(register_temp_table)
        if persist:
            df.persist()
        return df

    def get_spark_session(self):
        if not self._spark_session:
            self._spark_session = get_spark_session(self.master, self.options)
        return self._spark_session

    def get_spark_context(self):
        return self.get_spark_session().sparkContext

    def parallelize(self):
        return self.get_spark_context()

    def create_data_frame(self, c, schema="COLUMN string", register_temp_table=None, persist=True):
        df = self.get_spark_session().createDataFrame([(i,) for i in c], schema)
        if register_temp_table:
            df.registerTempTable(register_temp_table)
        if persist:
            df.persist()
        return df

    def __init__(self, key, master, options=None):
        super().__init__(key=key)
        self.master=master
        self.options = options
        self._sql = None
        self._spark_session = None
        self.spark_tools = spark_tools

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
        #print("Query/table:"+query_or_table)
        df = cf.load()

        df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        return df

    def convert_pandas_df_to_spark(self, pandas_df, register_temp_table=None):
        return convert_pandas_df_to_spark(self.get_spark_session(), pandas_df, register_temp_table)



# Auxiliary functions
# def equivalent_type(f):
#     if f == 'datetime64[ns]':
#         return TimestampType()
#     elif f == 'int64':
#         return LongType()
#     elif f == 'int32':
#         return IntegerType()
#     elif f == 'float64':
#         return FloatType()
#     else:
#         return StringType()
#
# def equivalent_type_2(f):
#     if f == 'datetime64[ns]':
#         return DateType()
#     elif f == 'int64':
#         return LongType()
#     elif f == 'int32':
#         return IntegerType()
#     elif f == 'float64':
#         return FloatType()
#     else:
#         return StringType()


#
# def define_structure(string, format_type):
#     try:
#         typo = equivalent_type(format_type)
#     except:
#         typo = StringType()
#     return StructField(string, typo)
#
# def define_structure_2(string, format_type):
#     try:
#         typo = equivalent_type_2(format_type)
#     except:
#         typo = StringType()
#     return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
# def pandas_to_spark(spark, pandas_df):
#
#     try:
#         return spark.createDataFrame(pandas_df).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
#     except:
#         columns = list(pandas_df.columns)
#
#         struct_list = []
#         # infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
#         # pandas_df.apply(infer_type, axis=0)
#
#         #df_types = list(
#         #    pandas.DataFrame(pandas_df.apply(pandas.api.types.infer_dtype, axis=0)).reset_index().rename(
#         #        columns={'index': 'column', 0: 'type'}));
#         types = list(pandas_df.dtypes)
#         # pandas_df=pandas_df.astype(df_types)
#         for column, typo in zip(columns, types):
#             struct_list.append(define_structure(column, typo))
#         p_schema = StructType(struct_list)
#         try:
#             return spark.createDataFrame(pandas_df, p_schema).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
#         except:
#             struct_list = []
#             for column, typo in zip(columns, types):
#                 struct_list.append(define_structure_2(column, typo))
#             p_schema = StructType(struct_list)
#             print(p_schema)
#             return spark.createDataFrame(pandas_df, p_schema).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)


def convert_pandas_df_to_spark(spark, pandas_df, spark_register_name=None, convert_via_parquet=True, persist=True):
    if convert_via_parquet:
        tmp=tempfile.mkdtemp()
        path = os.path.join(tmp, 'temp_convert_pandas_df_to_spark.parquet')
        pandas_df.to_parquet(path)
        convertedToSpark=spark.read.parquet(path)
        logger.debug("Saved temp parquet file:" + path)
    else:
        convertedToSpark = DataFrameConverter().get_spark_df(spark, pandas_df)
    if spark_register_name is not None:
         convertedToSpark.registerTempTable(spark_register_name)
    if persist:
        convertedToSpark.persist()
    return convertedToSpark

def df_to_excel(filename):
    spark_tools.df_to_excel(filename)


def df_to_csv(dir):
    spark_tools.df_to_csv(dir)


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




class DataFrameConverter:
    """A class that takes in a pandas data frame and converts it to a spark data frame..."""

    dtypeHeader = 'dtype'
    actualHeader = 'actual'

    def debug(self):
        print(f"dataframe type{type(self.df)}")
        print(f"dtypeHeader{self.dtypeHeader}")

    def get_pdf_column_meta(self, column_name_set):
        column_meta = {}
        for ch in column_name_set:
            column_meta.update({ch: {
                self.dtypeHeader: str(self.df[ch].dtypes),
                self.actualHeader:  str(type(self.df[ch][0])).split("'")[1] if 0 in self.df[ch] else ""
            }})
        return column_meta

    def equivalent_type(self, dtype, actual, datetype=1):
        #print(dtype+" "+actual)
        if dtype == 'datetime64[ns]' and 'Timestamp' in actual:
            return TimestampType()
        elif dtype == 'datetime64[ns]':
            if datetype==1:
                return TimestampType()
            else:
                return DateType()
        elif dtype == 'int64':
            return LongType()
        elif dtype == 'int32':
            return IntegerType()
        elif dtype == 'float64':
            return FloatType()
        elif dtype == "object" and actual == "list":
            return ArrayType(StringType())
        elif dtype == "object" and actual == "datetime.date":
            return DateType()
        else:
            return StringType()

    def define_structure(self, column, tpe1, tpe2):
        try:
            typo = self.equivalent_type(tpe1, tpe2)
        except:
            typo = StringType()
            print("not ok match type, resorting to string")
        struct_field_return = StructField(column, typo)
        return struct_field_return

    def get_spark_df(self, spark, df):
        self.df = df
        meta = self.get_pdf_column_meta(self.df.columns)
        struct_list = []
        for x in meta:
            # tpe = col_attr(meta, str(x))
            tpe = [str(meta.get(x).get(self.dtypeHeader)), str(meta.get(x).get(self.actualHeader))]
            struct_list.append(self.define_structure(x, tpe[0], tpe[1]))
        p_schema = StructType(struct_list)
        return spark.createDataFrame(self.df, p_schema)