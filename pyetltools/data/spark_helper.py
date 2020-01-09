from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyetltools.config import config

def get_spark_session(spark_params):
    app_name = ""
    master = "local[*]"
    conf = SparkConf() \
        .setAppName(app_name) \
        .setMaster(master)
    for (key, value) in spark_params.items():
        conf = conf.set(key, value)

    spark = SparkSession.builder.config(conf=conf).getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    return spark;


def get_spark_params_from_config():
    config=config.get_by_group_name("SPARK")
    opts={}
    for c in config.values():
        opts[c.key.replace("SPARK/","")]= c.value
    return opts

def get_spark_session():
    get_spark_session(get_spark_params_from_config())