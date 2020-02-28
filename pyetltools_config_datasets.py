
#
# # DATASET MANAGER Config
from pyetltools.core import connector
from pyetltools.data.dataset_manager import Dataset, DatasetManager

datasets = list()
datasets.append(Dataset(
    key="WF_RELATIONS_FTST2",
    db_connector="DB/NZFTST2",
    table="META.WF_RELATIONS",
    data_source="FTST2_META",
    lazy_load=True
))

datasets.append(Dataset(
    key="WF_RELATIONS_PROD",
    db_connector="DB/NZPROD",
    table="META.WF_RELATIONS",
    data_source="PROD_META",
    lazy_load=True
))

datasets.append(Dataset(
    key="BEC_TASK_INST_RUN_ANALYSIS_FTST2",
    db_connector="DB/SQLFTST2_PC_REP",
    query="select * from [D00000TD10_PWC_REP_FTST2].[DBO].[BEC_TASK_INST_RUN_ANALYSIS] where start_time > '{start_time}'",
    query_arguments={'start_time': '2019-06-01 17:00:00.000'},
    data_source=None,
    lazy_load=True,
    cache_in_hive=True
))

datasets.append(Dataset(
    key="BEC_TASK_INST_RUN_ANALYSIS_PROD",
    db_connector="DB/SQLPROD_PC_REP",
    query="select * from [D00000PD10_PWC_REP_PROD].[DBO].[BEC_TASK_INST_RUN_ANALYSIS] where start_time > '{start_time}'",
    query_arguments={'start_time': '2019-06-01 17:00:00.000'},
    data_source=None,
    lazy_load=True,
    cache_in_hive=True
))

connector.add(DatasetManager(key="DS", datasets=datasets, spark_connector="SPARK"))

