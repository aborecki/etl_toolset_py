import logging

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyetltools.core.connection import Connection
from pyetltools.data.spark.config import SparkConfig


class SparkConnection(Connection):

    def __init__(self, config: SparkConfig):
        super().__init__(config)

    def get_spark_session(self, spark_params=None):

        if spark_params is None:
            spark_params=self.get_spark_params_from_config()
        app_name = ""
        master = "local[*]"
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster(master)
        for (key, value) in spark_params.items():
            conf = conf.set(key, value)

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def get_spark_params_from_config(self):
        opts={}
        for c in self.config.options:
            opts[c.key] = c.value
        return opts

