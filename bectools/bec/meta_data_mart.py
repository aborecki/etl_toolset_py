from pyspark.sql.session import SparkSession
from time import gmtime, strftime

import pyetltools.core.connector as c
from pyetltools.data.dataset_manager import Dataset
from pyetltools.data.db_connector import DBConnector
from pyetltools.data.spark.spark_connector import SparkConnector

spark_sql:SparkSession=c.get("SPARK").get_spark_session().sql


def refresh_table(spark_sql:SparkSession, db_connector:DBConnector, table_name, query):
    current_timestamp=strftime("%Y%m%d_%H%M%S", gmtime())
    spark_sql.sql(f"alter table {table_name} renate to {table_name}_"+current_timestamp)

def load_wf_relations():
    ftst2_wf_relations_ds=Dataset(key="FTST2_WF_RELATIONS", table="META.WF_RELATIONS", db_connector=c.get("DB/NZFTST2"),
                            data_source="FTST2_META",
                            cache_in_hive=True,
                            hive_schema="STAGE",
                            hive_archive_schema="STAGE_ARCHIVE", lazy_load=False )

    prod_wf_relations_ds=Dataset(key="PROD_WF_RELATIONS", table="META.WF_RELATIONS", db_connector=c.get("DB/NZPROD"),
                            data_source="FTST2_META",
                            cache_in_hive=True,
                            hive_schema="STAGE",
                            hive_archive_schema="STAGE_ARCHIVE", lazy_load=False )

    ftst2_bec_task_inst_run =Dataset(key="FTST2_BEC_TASK_INST_RUN", query="""
        select * from [dbo].[BEC_TASK_INST_RUN]
        where START_TIME > '2019-01-01'""", db_connector=c.get("DB/SQLFTST2_PC_REP"),
                            data_source="D00000TD10_PWC_REP_FTST2",
                            cache_in_hive=True,
                            hive_schema="STAGE",
                            hive_archive_schema="STAGE_ARCHIVE", lazy_load=False )


    prod_bec_task_inst_run=Dataset(key="PROD_BEC_TASK_INST_RUN", query="""
        select * from [dbo].[BEC_TASK_INST_RUN]
        where START_TIME > '2019-01-01'""", db_connector=c.get("DB/SQLPROD_PC_REP"),
                            data_source="D00000PD10_PWC_REP_PROD",
                            cache_in_hive=True,
                            hive_schema="STAGE",
                            hive_archive_schema="STAGE_ARCHIVE", lazy_load=False )

    print(prod_bec_task_inst_run.get_spark_df().count())



load_wf_relations()