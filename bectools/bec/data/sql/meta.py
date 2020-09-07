def sql_ftst2_bank_koncern():
    return "select * from ftst2_meta.meta.CNF_KONCERN"


def release_status_with_banks_sql():
    """
    SQL query: join between release_status_table, release_status_table_workflow, release_status_workflow and release_status_workflow
    for ftst2 database.
    """
    return  f"""SELECT 'D' 
       || Lpad(bank_r, 5, '0') 
       || SUBSTRING(ms_dbname, 7, 20) AS MS_DBNAME, 
         MS_TABLENAME, 
       NZ_TABLENAME, 
       NZ_TABLESCHEMA, 
       TABLE_TYPE,
       RENAME_STATUS, 
       PUSH_TO_MSSQL, 
       IS_INITABLE,
       FOLDER_NAME, 
       C.WORKFLOW_NAME, 
       KONV_FOLDER_NAME, 
       KONV_WORKFLOW_NAME, 
       BANK_I, 
       BANK_R, 
       KONCERN_BANK_R,
       RELEASE
FROM   mmdb.admin.release_status_table a 
       INNER JOIN mmdb.admin.release_status_table_workflow b USING (table_id) 
       INNER JOIN mmdb.admin.release_status_workflow c USING (workflow_id) 
       INNER JOIN ftst2_meta.meta.cnf_workflow_bank d ON d.workflow_name = c.konv_workflow_name 
WHERE  d.valid_to > CURRENT_DATE"""


def release_status_sql():
    """
    SQL query: join between release_status_table, release_status_table_workflow, release_status_workflow and release_status_workflow
    for ftst2 database.
    """
    return  f"""SELECT *
        FROM   mmdb.admin.release_status_table a 
               INNER JOIN mmdb.admin.release_status_table_workflow b USING (table_id) 
               INNER JOIN mmdb.admin.release_status_workflow c USING (workflow_id)"""

def release_status_tables(condition):
    """
    SQL query: join between release_status_table, release_status_table_workflow, release_status_workflow and release_status_workflow
    for ftst2 database.
    """
    return  f"""SELECT *
        FROM   mmdb.admin.release_status_table a 
               INNER JOIN mmdb.admin.release_status_table_workflow b USING (table_id) 
               INNER JOIN mmdb.admin.release_status_workflow c USING (workflow_id) 
        WHERE  {condition} """

def wf_relations(release):
    pass;