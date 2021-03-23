def get_wf_metadata_extension_values(db_con):
    return db_con.query("""
SELECT s.SUBJ_NAME, 
		se.SERVER_NAME,
		t.task_name as WORKFLOW_NAME,
		convert(varchar(2000),a.attr_value) as PARAMFILENAME,
		m.METADATA_EXTN_NAME, 
		convert(varchar(2000),m.METADATA_EXTN_VALUE) as METADATA_EXTN_VALUE
	FROM dbo.REP_METADATA_EXTNS m 
		INNER JOIN dbo.OPB_SUBJECT s ON m.SUBJECT_ID = s.SUBJ_ID 
		INNER JOIN dbo.OPB_WORKFLOW w ON m.METADATA_EXTN_OBJECT_ID = w.WORKFLOW_ID and w.VERSION_NUMBER = m.VERSION_NUMBER
		INNER JOIN dbo.OPB_SERVER_INFO se ON w.SERVER_ID = se.SERVER_ID
		INNER join dbo.OPB_TASK t on w.workflow_id = t.task_id and w.VERSION_NUMBER = t.VERSION_NUMBER
		INNER JOIN dbo.OPB_TASK_ATTR a on t.task_id = a.task_id and a.VERSION_NUMBER = t.VERSION_NUMBER 
	WHERE --m.METADATA_EXTN_NAME LIKE 'OPC%' AND 
	    m.OBJECT_TYPE_NAME = 'workflow'  AND NOT s.SUBJ_NAME LIKE 'Z_%'
		and	a.ATTR_ID = 1
		and t.IS_VISIBLE = 1
		--AND NOT (METADATA_EXTN_NAME = 'OPC_jobname' AND ((METADATA_EXTN_VALUE LIKE 'UDFASE%' OR METADATA_EXTN_VALUE LIKE '') AND t.task_name!='_____OPC_dummy'))""")


def get_metadata_extension_values(db_con):
    return db_con.query("""
    SELECT s.SUBJ_NAME, 
		m.METADATA_EXTN_NAME, 
		convert(varchar(2000),m.METADATA_EXTN_VALUE) as METADATA_EXTN_VALUE,
		m.*
	FROM dbo.REP_METADATA_EXTNS m 
		INNER JOIN dbo.OPB_SUBJECT s ON m.SUBJECT_ID = s.SUBJ_ID 
    """)