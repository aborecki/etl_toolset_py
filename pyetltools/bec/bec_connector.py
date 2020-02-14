from pyetltools.bec.model.entities import ReleaseStatusWorkflow, ReleaseStatusTable
from pyetltools.core import connector
from pyetltools.core.connector import Connector
import pyetltools.bec.data.sql.meta as sql_meta
import pyetltools.bec.data.sql.recon as sql_recon



class BECConnector(Connector):


    def __init__(self, key, infa):
        super().__init__(key)

    class Deploy:
        def deploy_infa_by_label(self, target_env):
            pass


    #
    #
    # def get_ftst2_db_connector(self):
    #     return  connector.get("DB/NZFTST2").set_data_source("MMDB")
    #
    # def get_bank_koncern_map(self):
    #     sql= sql_meta.sql_ftst2_bank_koncern()
    #     print("SQL:\n"+sql)
    #     return self.get_ftst2_db_connector().run_query_spark_dataframe(sql)
    #
    # def recon_add_aggregate_foreign_key(self, table_name, column_name):
    #     sql = sql_recon.ftst2_insert_aggregate_foreign_key(table_name, column_name)
    #     print("SQL:\n"+sql)
    #     return self.get_ftst2_db_connector().execute_statement(sql)
    #
    # def recon_remove_aggregated_results_for_table_name(self, table_name, date):
    #     sql = self.get_ftst2_db_connector().ftst2_delete_recon_results_for_table(table_name, date)
    #     print("SQL:\n" + sql)
    #     return self.get_ftst2_db_connector().execute_statement(sql)
    #
    # def dr_prod_meta_dr_restore_log(self, table_name, date):
    #     sql = sql_meta.prod_meta_dr_restore_log(date)
    #     print("SQL:\n" + sql)
    #     return self.get_ftst2_db_connector().execute_statement(sql)
    #
    # def sqlerver_wflow_run_inst(self, wf_run_id):
    #     sql = self.sql.monitoring.sqlerver_wflow_run_inst(wf_run_id)
    #     print("SQL:\n" + sql)
    #     return self.get_ftst2_db_connector().run_query_pandas_dataframe(sql)
    #
    # def get_release_status_workflow_sa(self):
    #     session= self.get_ftst2_db_connector().sa_get_session()
    #     return session.query(ReleaseStatusWorkflow)
    #
    # def get_release_status_table_sa(self):
    #     session= self.get_ftst2_db_connector().sa_get_session()
    #     return session.query(ReleaseStatusTable)
