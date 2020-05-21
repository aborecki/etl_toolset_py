
#
# # DATASET MANAGER Config
from pyetltools.core import connector
from pyetltools.data.dataset_manager import Dataset, DatasetManager


connector.add(Dataset(
    key="DS/FTST2/NZ/WF_RELATIONS",
    db_connector="DB/FTST2/NZ",
    table="META.WF_RELATIONS",
    data_source="FTST2_META",
    lazy_load=True,
    cache_in_hive=True
))

connector.add(Dataset(
    key="DS/PROD/NZ/WF_RELATIONS",
    db_connector="DB/PROD/NZ",
    table="META.WF_RELATIONS",
    data_source="PROD_META",
    lazy_load=True,
    cache_in_hive=True
))


connector.add(Dataset(
    key="DS/FTST2/NZ/CNF_KONCERN",
    db_connector="DB/FTST2/NZ",
    table="META.CNF_KONCERN",
    data_source="FTST2_META",
    lazy_load=True,
    cache_in_hive=False
))

connector.add(Dataset(
    key="DS/FTST2/SQL_PC_REP/BEC_TASK_INST_RUN_ANALYSIS",
    db_connector="DB/FTST2/SQL/PC_REP",
    query="select * from [D00000TD10_PWC_REP_FTST2].[DBO].[BEC_TASK_INST_RUN_ANALYSIS] where start_time > '{start_time}' ",
    query_arguments={"start_time": '2020-02-01 17:00:00.000'},
    data_source="D00000TD10_PWC_REP_FTST2",
    lazy_load=True,
    cache_in_hive=False
))

connector.add(Dataset(
    key="DS/PROD/SQL_PC_REP/BEC_TASK_INST_RUN_ANALYSIS",
    db_connector="DB/PROD/SQL/PC_REP",
    query="select * from [D00000PD10_PWC_REP_PROD].[DBO].[BEC_TASK_INST_RUN_ANALYSIS] where start_time > '{start_time}'",
    query_arguments={'start_time': '2020-02-01 17:00:00.000'},
    data_source='D00000PD10_PWC_REP_PROD',
    lazy_load=True,
    cache_in_hive=False
))

connector.add(Dataset(
    key="DS/PROD/SQL_PC_REP/BEC_TASK_INST_RUN",
    db_connector="DB/PROD/SQL/PC_REP",
    query="select * from [D00000PD10_PWC_REP_PROD].[DBO].[BEC_TASK_INST_RUN] where start_time > '{start_time}'",
    query_arguments={'start_time': '2020-02-01 17:00:00.000'},
    data_source='D00000PD10_PWC_REP_PROD',
    lazy_load=True,
    cache_in_hive=True
))



connector.add(Dataset(
    key="DS/PROD/DB2/CD99/XXRTOPG_35_JOBS_DEPENDENCIES",
    db_connector="DB/PROD/DB2/CD99",
    query="""SELECT 
    "XXRDATCLIENT", "XXRDATENV", "OPCSUBSYS", "OPGPREDKEY", "OPGSUCCKEY", "OPGADRID", "OPGADROPNO", "OPGADRWSID", 
    "OPGADROPJN", "OPGEXTDEP", "OPGADRADDESC", "OPGADROPDESC", "OPGADRSTAT", "OPGADRFROM", "OPGADRTO", "OPGADROPEDE", 
    "OPGOPCOID", "OPGOPCODESC", "OPGOPCOCOUNT", "OPGOPCOSIMPNO", "OPGOPCORULET", "OPGOPSCPRETYP", "OPGOPSCPRELOG", "OPGOPSCVALRC", 
    "OPGOPSCVALRC2", "OPGOPSCVALST", "ADROPSCSTEP", "ADROPSCPSTEP", "ADROPDURSECS"
FROM
    CD99."XXRTOPG_35"
WHERE
    ("OPGADROPJN" LIKE '{opc_job_id}') order by opgpredkey, opgsucckey""",
    query_arguments={'opc_job_id': '%'},
    lazy_load=True,
    cache_in_hive=True
))


connector.add(Dataset(
    key="DS/PROD/DB2/CD99/XXRTOPG_35_JOBS_SCHEDULING",
    db_connector="DB/PROD/DB2/CD99",
    query="""SELECT 
    T153."ADROPJN", T153."ADRID", T153."ADRSTAT", T153."ADRWSID", T153."ADROPNO", T153."ADRTO", T156."ADRRPER", T156."ADRRULE", 
    T156."ADRRTYPE", T156."ADRRARRI", T156."ADRRREVYRR", T156."ADRRREVYET", T156."ADRRDAY", T156."ADRRTIME", T156."ADRRFROM", T156."ADRROUT", 
    T156."ADRRNPOS", T156."ADRRNNEG", T156."ADRRDAYP", T156."ADRRDAYN", T156."ADRRRDEF", T153."ADROPTIM", T153."ADROPCAN", T153."ADROPSTD", 
    T153."ADROPSTT", T153."ADROPDD", T153."ADROPDT", T153."OPCSUBSYS", T156."ADRRDESC"
FROM
    CD99."XXRTOP_35" T153
JOIN
    CD99."XXRTRUN_35" T156 ON (T156."ADRID" = T153."ADRID") AND (T156."ADRSTAT" = T153."ADRSTAT") AND (T156."ADRTO" = T153."ADRTO") AND (T156."OPCSUBSYS" = T153."OPCSUBSYS")
WHERE
    ("T153"."ADROPJN" LIKE '{opc_job_id}')""",
    query_arguments={'opc_job_id': '%'},
    lazy_load=True,
    cache_in_hive=True
))



