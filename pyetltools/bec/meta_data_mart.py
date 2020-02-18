from pyspark.sql.session import SparkSession
from time import gmtime, strftime

import pyetltools.core.connector as c
from pyetltools.data.db_connector import DBConnector
from pyetltools.data.spark.spark_connector import SparkConnector

spark_sql:SparkSession=c.get("SPARK").get_spark_session().sql


def refresh_table(spark_sql:SparkSession, db_connector:DBConnector, table_name, query):
    current_timestamp=strftime("%Y%m%d_%H%M%S", gmtime())
    spark_sql.sql(f"alter table {table_name} renate to {table_name}_"+current_timestamp)

def load_wf_relations():
    db_nzftst2: DBConnector=c.get("DB/NZFTST2")
    db_nzftst2.set_data_source("FTST2_META").run_query_spark_dataframe("select * from admin.wf_relations")
    .sql("select * from admin.wf_relations").write.saveAsTable("")