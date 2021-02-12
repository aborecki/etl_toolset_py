from pyetltools import logger

def get_persistence_values(db_con, workflow_name):
    query=rf"""
SELECT __OPB_Subject.SUBJ_NAME AS Folder,
           __OPB_TASK_Workflow.TASK_NAME AS Workflow,
           __OPB_TASK_INST.INSTANCE_NAME AS Session,
           __OPB_MAP_PARMVAR.PV_NAME AS PersistentVariable,
           convert(varchar(100),__OPB_MAP_PERSISVAL.PV_VALUE) AS Value,
            isnull(__OPB_MAP_PERSISVAL.RUNINST_NAME, '<NULL>') as RUNINST_NAME,
case fdt.DTYPE_GROUP_CODE 
                         when 'N' then RIGHT( SPACE(100) + convert(varchar(100),__OPB_MAP_PERSISVAL.PV_VALUE), 100)
                         when 'D' then convert(varchar(100), convert(datetime, convert(varchar(23),__OPB_MAP_PERSISVAL.PV_VALUE), 101), 121)
                         else convert(varchar(100),__OPB_MAP_PERSISVAL.PV_VALUE)
            end Sort_Value,
            __OPB_Subject.SUBJ_NAME + ';' + __OPB_TASK_Workflow.TASK_NAME + ';' + __OPB_TASK_INST.INSTANCE_NAME + ';' + __OPB_MAP_PARMVAR.PV_NAME + ';' + isnull(convert(varchar(2000), __OPB_MAP_PERSISVAL.PV_VALUE), '')+';'+isnull(__OPB_MAP_PERSISVAL.RUNINST_NAME, 'NULL') as PV_FILE 
FROM
           dbo.OPB_TASK AS __OPB_TASK_Workflow 
INNER JOIN
            dbo.OPB_TASK_INST AS __OPB_TASK_INST 
ON __OPB_TASK_Workflow.TASK_ID = __OPB_TASK_INST.WORKFLOW_ID AND 
            __OPB_TASK_Workflow.VERSION_NUMBER = __OPB_TASK_INST.VERSION_NUMBER AND
            __OPB_TASK_Workflow.IS_VISIBLE = 1 AND 
            __OPB_TASK_Workflow.TASK_TYPE = 71 AND
            __OPB_TASK_INST.TASK_TYPE = 68
INNER JOIN
            dbo.OPB_TASK AS __OPB_TASK_Session 
ON __OPB_TASK_INST.TASK_ID = __OPB_TASK_Session.TASK_ID AND 
            __OPB_TASK_INST.TASK_TYPE = __OPB_TASK_Session.TASK_TYPE AND
            __OPB_TASK_Session.IS_VISIBLE = 1 AND
            __OPB_TASK_Session.TASK_TYPE = 68
INNER JOIN
            dbo.OPB_SESSION AS __OPB_SESSION 
ON __OPB_TASK_Session.VERSION_NUMBER = __OPB_SESSION.VERSION_NUMBER AND 
            __OPB_TASK_Session.TASK_ID = __OPB_SESSION.SESSION_ID
INNER JOIN
            dbo.OPB_MAPPING AS __OPB_MAPPING 
ON __OPB_SESSION.MAPPING_ID = __OPB_MAPPING.MAPPING_ID AND
            __OPB_MAPPING.IS_VISIBLE = 1
INNER JOIN
            dbo.OPB_SUBJECT AS __OPB_SUBJECT 
ON __OPB_SUBJECT.SUBJ_ID = __OPB_TASK_Workflow.SUBJECT_ID 

INNER JOIN
            dbo.OPB_MAP_PARMVAR AS __OPB_MAP_PARMVAR 
ON __OPB_MAP_PARMVAR.MAPPING_ID = __OPB_MAPPING.MAPPING_ID AND 
            __OPB_MAP_PARMVAR.SUBJECT_ID = __OPB_MAPPING.SUBJECT_ID AND 
            __OPB_MAP_PARMVAR.VERSION_NUMBER = __OPB_MAPPING.VERSION_NUMBER AND
            __OPB_MAP_PARMVAR.PV_FLAG <> 2
inner JOIN 

(SELECT * FROM dbo.OPB_MAP_PERSISVAL) AS __OPB_MAP_PERSISVAL
ON __OPB_MAP_PERSISVAL.SUBJECT_ID = __OPB_MAP_PARMVAR.SUBJECT_ID AND 
            __OPB_MAP_PERSISVAL.MAPPING_ID = __OPB_MAP_PARMVAR.MAPPING_ID AND 
            __OPB_MAP_PERSISVAL.PV_ID = __OPB_MAP_PARMVAR.PV_ID AND 
            __OPB_MAP_PERSISVAL.SESSION_INST_ID = __OPB_TASK_INST.INSTANCE_ID
inner join dbo.REP_FLD_DATATYPE fdt on fdt.DTYPE_NUM = __OPB_MAP_PARMVAR.PV_DATATYPE

WHERE (1 = 1)
     and __OPB_TASK_Workflow.TASK_NAME  like '%\_[0-9]%' escape '\'                    
            and __OPB_MAP_PERSISVAL.RUNINST_NAME = 'NULL'
      and __OPB_Subject.SUBJ_NAME not like 'z_%'
--and __OPB_Subject.SUBJ_NAME = 'RAP'
--and __OPB_Subject.SUBJ_NAME = 'EDW_NZ'
--and __OPB_TASK_INST.INSTANCE_NAME = 's_m_TEDW2500POSTERING'
and  __OPB_TASK_Workflow.TASK_NAME like  '{workflow_name}'
--and __OPB_TASK_INST.INSTANCE_NAME like 's_m_TEJD0020%'
--and __OPB_MAP_PARMVAR.PV_NAME = '$$LAST_KORREKTION_I'
order by 2
"""
    logger.info(query)
    return db_con.query_pandas(query)

def get_persistence_values_hist(db_con, workflow_name):
    query=f"""
SELECT
    osu.SUBJ_NAME SUBJECT_NAME, osu.SUBJ_ID SUBJECT_ID, 
	otw.TASK_ID WORKFLOW_ID, otw.TASK_NAME WORKFLOW_NAME, otw.VERSION_NUMBER WORKFLOW_VERSION,
	oti.INSTANCE_ID, oti.INSTANCE_NAME, 
	ots.TASK_ID SESSION_ID, ots.TASK_NAME SESSION_NAME, ots.VERSION_NUMBER SESSION_VERSION,
	om.MAPPING_ID, om.MAPPING_NAME, om.VERSION_NUMBER MAPPING_VERSION,
	omp.PV_NAME, omp.PV_ID, omp.PV_DEFAULT,
	ompv.PV_VALUE, cast(ompv.LAST_SAVED as datetime) PV_LAST_SAVED,
	osu.SUBJ_NAME + ';' + otw.TASK_NAME + ';' + oti.INSTANCE_NAME + ';' + omp.PV_NAME + ';' + isnull(convert(varchar(2000), ompv.PV_VALUE), '') as PV_FIL_VAERDI 
FROM         dbo.OPB_TASK_INST AS oti INNER JOIN
                      dbo.OPB_TASK AS ots ON ots.TASK_ID = oti.TASK_ID AND oti.TASK_TYPE = ots.TASK_TYPE AND ots.IS_VISIBLE = 1 INNER JOIN
                      dbo.OPB_SESSION AS os ON ots.VERSION_NUMBER = os.VERSION_NUMBER AND ots.TASK_ID = os.SESSION_ID INNER JOIN
                      dbo.OPB_MAPPING AS om ON os.MAPPING_ID = om.MAPPING_ID AND om.IS_VISIBLE = 1 INNER JOIN
                      dbo.OPB_TASK AS otw ON otw.TASK_ID = oti.WORKFLOW_ID AND otw.IS_VISIBLE = 1 AND oti.VERSION_NUMBER = otw.VERSION_NUMBER INNER JOIN
                      dbo.OPB_SUBJECT AS osu ON osu.SUBJ_ID = otw.SUBJECT_ID INNER JOIN
                      dbo.OPB_MAP_PARMVAR AS omp ON omp.MAPPING_ID = om.MAPPING_ID AND omp.SUBJECT_ID = om.SUBJECT_ID AND 
                      omp.VERSION_NUMBER = om.VERSION_NUMBER LEFT OUTER JOIN
                      dbo.OPB_MAP_PERSISVAL_hist AS ompv ON ompv.SUBJECT_ID = omp.SUBJECT_ID AND ompv.MAPPING_ID = omp.MAPPING_ID AND 
                      ompv.PV_ID = omp.PV_ID AND oti.INSTANCE_ID = ompv.SESSION_INST_ID
WHERE     (omp.PV_FLAG % 2 = 1)  and otw.TASK_NAME like '{workflow_name}' order by PV_LAST_SAVED"""
    logger.info(query)
    return db_con.query_pandas(query)