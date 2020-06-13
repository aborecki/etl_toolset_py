from pyetltools import connectors as con
from pyetltools.data.dataset import Dataset

"""

WORKFLOW
WORKFLOW_INSTANCE -> WORKFLOW, attr: ENV
WORKFLOW_RUN -> WORKFLOW_INSTANCE

TABLE (just SCHEMA,TABLE_NAME pair)
TABLE_INSTANCE -> TABLE

WORKFLOW_TABLE -> TABLE, WORKFLOW
WORKFLOW_DEPT -> WORKFLOW, WORKFLOW   attr: DEPT_TYPE (WF_REL, OPC, other), TABLE (optional)

"""






def get_workflows_dataset(data_source, env, workflow_name="%", subject_area="%", opc_jobname="%"):
    return Dataset(
        key="DATASOURCES_"+env+"_WORKFLOWS",
        db_connector="DB/"+env+"/SQL/PC_REP",
        query="""
        with r as (
        select distinct 'PROD' ENV, SUBJECT_AREA, WORKFLOW_NAME, cast(metadata_extn_value as varchar) OPC_JOBNAME,
         case when cast(metadata_extn_value as varchar) like '%$%' then 'N' else 'Y' end IS_CLONED,
		 replace(
		  replace(
		    replace(
		     replace(cast(metadata_extn_value as varchar), '$OPC_MILJOE$',''),
		  '$KOERSEL$',''),
		  '$BIX_MILJOE$',''),
		  '$BANKNR$','') OPC_JOBNAME_CONST
         from [dbo].[REP_WORKFLOWS] wf
        LEFT OUTER JOIN 
        (
            select * from [REP_METADATA_EXTNS] ext where not exists (
                    select 1 from [REP_METADATA_EXTNS] older where
                    ext.METADATA_EXTN_OBJECT_ID=older.METADATA_EXTN_OBJECT_ID AND
                    ext.METADATA_EXTN_OBJECT_TYPE= older.METADATA_EXTN_OBJECT_TYPE AND 
                    ext.SUBJECT_ID =  older.SUBJECT_ID AND
                    ext.VERSION_NUMBER < older.VERSION_NUMBER AND
                    ext.METADATA_EXTN_NAME =  older.METADATA_EXTN_NAME AND
                    ext.LINE_NO = older.LINE_NO
                )
        )    ext ON (wf.SUBJECT_ID=ext.SUBJECT_ID AND wf.WORKFLOW_ID=ext.METADATA_EXTN_OBJECT_ID and METADATA_EXTN_NAME='OPC_jobName')
        WHERE workflow_name like '{workflow_name}' AND subject_area like '{subject_area}' and   metadata_extn_value like '{opc_jobname}')
         select r.*,  max(parent.workflow_name) PARENT_WORFKLOW , count(*) DEBUG_PARENT_WORFKLOW_CNT
         from r left outer join r parent 
          on (r.workflow_name like parent.workflow_name+'%' and 
            (r.opc_jobname like '%'+parent.OPC_JOBNAME_CONST+'%' or  r.opc_jobname='UDFASET') and r.is_cloned='Y' and (parent.is_cloned='N' or parent.opc_jobname='UDFASET')
            and parent.workflow_name <> r.workflow_name)
        group by r.ENV, r.SUBJECT_AREA, r.WORKFLOW_NAME, r.OPC_JOBNAME, r.OPC_JOBNAME_CONST, r.IS_CLONED
        """,
        query_arguments={"workflow_name": workflow_name,
                          "subject_area":subject_area,
                          "opc_jobname":opc_jobname},
        data_source=data_source
    )

# def refresh_workflows():
#     get_workflows_dataset("%","%","D00000PD10_PWC_REP_PROD", "PROD").refresh_spark_df_from_source()




def get_workflows_pd_df( workflow_name="%", subject_area="%", opc_jobname="%" ):
    ds= get_workflows_dataset("D00000PD10_PWC_REP_PROD", "PROD",workflow_name, subject_area, opc_jobname)
    prod_workflows=ds.get_pandas_df()
    df=prod_workflows
    return df

# def get_workflows(refresh=False):
#     from pyspark.sql import functions as f
#     ds= get_workflows_dataset("%","%","D00000PD10_PWC_REP_PROD", "PROD")
#     if refresh:
#         ds.refresh_spark_df_from_source()
#     prod_workflows=ds.get_spark_df()
#     df=prod_workflows
#
#     df.registerTempTable("df")
#     # add parent_wf column for cloned workflows)
#     ret=con.SPARK.sql("""
#         select df.*,  max(df_parent.workflow_name) parent_worfklow
#             from df left outer join df df_parent
#             on (df.workflow_name like df_parent.workflow_name+'%' and
#             regexp_replace(df.opc_jobname,'$[\$]+$','') like '%'+df_parent.opc_jobname+'%' and df.is_cloned='Y')
#         group by df.env, df.subject_area, df.workflow_name, df.opc_jobname, df.is_cloned
#         """)
#     return ret
#     #dev_workflows = get_workflows_dataset("%", "%", "D00000TD10_PWC_REP_DEV","DEV").get_spark_df()
#     #return prod_workflows.unionAll(dev_workflows).withColumn("IS_CLONED",f.when(f.col("OPC_JOBNAME").contains("$"),'N').otherwise('Y') )

def get_opc_dependencies_dataset( opc_jobname="%"):
    return Dataset(
        key="DATASOURCES_JOBS_DEPENDENCIES",
        db_connector="DB/PROD/DB2/CD99",
        query="""SELECT   distinct
    job."OPGADROPJN" as OPC_JOBNAME, job."OPGADRADDESC", job."OPGADROPDESC", job."OPGADRFROM", job."OPGADRTO", 
    job_succ.OPGADROPJN as OPC_JOBANAME_SUCC,  job_pred.OPGADROPJN as OPC_JOBANAME_PRED
    FROM
        CD99."XXRTOPG_35" job left outer join CD99."XXRTOPG_35" job_succ on (job.OPGSUCCKEY=job_succ.OPGPREDKEY)
       left outer join CD99."XXRTOPG_35" job_pred on (job_pred.OPGSUCCKEY=job.OPGPREDKEY)
    WHERE
        (job."OPGADROPJN" LIKE '{opc_job_id}') order by job."OPGADROPJN",  job_succ.OPGADROPJN""",
        query_arguments={'opc_job_id': opc_jobname}
    )



def get_opc_dependencies_pd_df(opc_jobname="%" ):
    ds= get_opc_dependencies_dataset(opc_jobname)

    import pyspark.sql.functions as f
    df=ds.get_spark_df()
    #df=df.groupby(["OPC_JOBNAME","OPGADRADDESC","OPGADROPDESC","OPGADRFROM","OPGADRTO"]).agg(f.concat_ws(", ", f.collect_set(df.OPC_JOBANAME_SUCC)).alias("OPC_JOBANAME_SUCC"),
    #                                                                                         f.concat_ws(", ", f.collect_set(df.OPC_JOBANAME_PRED)).alias("OPC_JOBANAME_PRED"))
    df=df.toPandas()

    wf=get_workflows_pd_df(opc_jobname=opc_jobname)

    merged = df.merge(wf, left_on='OPC_JOBANAME_SUCC',  right_on='OPC_JOBANAME')
    return merged

def get_workflow_instances():
    pass


def get_workflow_runs_dataset(data_source, start_time):
    return Dataset(
        db_connector="DB/PROD/SQL/PC_REP",
        query="select * from [DBO].[BEC_TASK_INST_RUN_ANALYSIS] where start_time > '{start_time}'",
        query_arguments={'start_time': start_time},
        data_source=data_source,
        cache_in_filesystem=True
    )

def get_workflow_runs():
    return get_workflow_runs_dataset('D00000PD10_PWC_REP_PROD', start_time="2020-06-01 17:00:00.000").get_spark_df()