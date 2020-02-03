def aggregates_mdw(wf_name_new, validation_i_lt_id, filter="1=1"):
    return f"""
            select *
        from mmdb.ADMIN.tb_AGGREGATES_MDW
        where validation_i = '{validation_i_lt_id}' and
        {filter} and
        substring(tablename,2) in
         (
         select substring(table_name,2)
         from ftst2_meta.meta.wf_relations
         where table_type = 'TARGET' and object_name in
         (
         select konv_workflow_name
         from mmdb.ADMIN.RELEASE_STATUS_WORKFLOW
         where konv_workflow_name like UPPER('{wf_name_new}' || '%')
        )
        )
        and (banknr = '000' or banknr in
         (
         select distinct koncern_bank_r
         from ftst2_meta.META.cnf_workflow_bank
         where workflow_name like UPPER('{wf_name_new}' || '%')
        ))
        and (LOESNING = 'R1_3' or (LOESNING = 'PUSH_R3_4' and BANKNR in (20,29,41,62,369) and
        substring(tablename,2) in 
        (
            select substring(nz_tablename,2) 
            from mmdb.ADMIN.release_status_table 
            where push_to_mssql = 'Y'
            ))) 
        order by validation_i desc, loesning desc;
            """


def aggregates_mdw_errors(wf_name_new, validation_i_lt_id):
    return aggregates_mdw(wf_name_new, validation_i_lt_id, "(error_state <> 0 or rowcounts_realized = 0)")


def exec_temp_access(group, comment):
    return "exec sp_temp_access('{group}','{comment}')"


def ftst2_insert_aggregate_foreign_key(table_name, column_name):
    return f"""
        INSERT INTO mmdb.admin.tb_aggregate_foreign_keys 
                    (tablename, 
                     columnname) 
        (SELECT '{table_name}', 
                '{column_name}' 
         WHERE  NOT EXISTS (SELECT 1 
                            FROM   mmdb.admin.tb_aggregate_foreign_keys 
                            WHERE  tablename = '{table_name}' 
                                   AND columnname = '{column_name}')) ;
        commit;
    """

def ftst2_delete_recon_results_for_table(table_name, date):
    return f"""
    delete from mmdb.ADMIN.TB_AGGREGATES_MDW where validation_i = {date}
and tablename in ('{table_name}');

delete from mmdb.ADMIN.TB_AGGREGATES_MDW_SCRIPT where PUSH_LOG_I = {date}
and tablename in ('{table_name}');

delete from mmdb.ADMIN.TB_AGGREGATES_MDW_EXPECTED where validation_i = {date}
and tablename in ('{table_name}');

delete from mmdb.ADMIN.TB_AGGREGATES_MDW_REALIZED where validation_i = {date}
and tablename in ('{table_name}');
commit;
    """


def prod_meta_dr_restore_log(date):
    return f"""
    select * 
    from META.DR_RESTORELOG where start_date > '{date}';
    """


