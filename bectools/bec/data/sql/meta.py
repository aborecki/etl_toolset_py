def sql_ftst2_bank_koncern():
    return "select * from ftst2_meta.meta.CNF_KONCERN"


def release_status(release):
    """
    SQL query: join between release_status_table, release_status_table_workflow, release_status_workflow and release_status_workflow
    for ftst2 database.
    """
    return  f"""SELECT 'D' 
       || Lpad(bank_r, 5, '0') 
       || SUBSTRING(ms_dbname, 7, 20) AS MS_DBNAME, 
         ms_tablename, 
       nz_tablename, 
       nz_tableschema, 
       rename_status, 
       push_to_mssql, 
       folder_name, 
       c.workflow_name, 
       konv_folder_name, 
       konv_workflow_name, 
       bank_i, 
       bank_r, 
       koncern_bank_r 
FROM   mmdb.admin.release_status_table a 
       INNER JOIN mmdb.admin.release_status_table_workflow b USING (table_id) 
       INNER JOIN mmdb.admin.release_status_workflow c USING (workflow_id) 
       INNER JOIN ftst2_meta.meta.cnf_workflow_bank d ON d.workflow_name = c.konv_workflow_name 
WHERE  c.RELEASE = '{release}' 
       AND b.table_type = 'TARGET' 
       AND d.valid_to > CURRENT_DATE"""

def wf_relations(release):
    pass;