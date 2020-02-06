import pyetltools
import pyetltools.bec.data.sql.recon as sql_recon
import pyetltools.bec.data.sql.meta as sql_meta
from pyetltools.bec.model.entities import *

from pyetltools.core import context
from pyetltools.data.config import DBConfig
from pyetltools.data.core.context import DBContext
from pyetltools.data.core.connection import DBConnection


class BECDBContext(DBContext):
    sql=pyetltools.bec.data.sql

    def exec_temp_access(self, group="", comment=""):
        print(self.execute_statement(f"exec sp_tempaccess('{group}','{comment}')"))

    def get_bank_koncern_map(self):
        sql= sql_meta.ftst2_bank_koncern()
        print("SQL:\n"+sql)
        return self.get_spark_dataframe(sql)

    def recon_add_aggregate_foreign_key(self, table_name, column_name):
        sql = sql_recon.ftst2_insert_aggregate_foreign_key(table_name, column_name)
        print("SQL:\n"+sql)
        return self.execute_statement(sql)

    def recon_remove_aggregated_results_for_table_name(self, table_name, date):
        sql = sql_recon.ftst2_delete_recon_results_for_table(table_name, date)
        print("SQL:\n" + sql)
        return self.execute_statement(sql)

    def dr_prod_meta_dr_restore_log(self, table_name, date):
        sql = sql_meta.prod_meta_dr_restore_log(date)
        print("SQL:\n" + sql)
        return self.execute_statement(sql)

    def sqlerver_wflow_run_inst(self, wf_run_id):
        sql = self.sqls.sqlerver_wflow_run_inst(wf_run_id)
        print("SQL:\n" + sql)
        return self.get_pandas_dataframe(sql)

    def get_release_status_workflow_sa(self):
        session= self.sa_get_session()
        return session.query(ReleaseStatusWorkflow)

    def get_release_status_table_sa(self):
        session= self.sa_get_session()
        return session.query(ReleaseStatusTable)


    def __init__(self, config: DBContext, connection: DBConnection):
        super().__init__(config, connection)


    @classmethod
    def create_from_config(cls, config: DBConfig):
        return BECDBContext(config, super().create_connection_from_config(config))


    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(DBConfig, BECDBContext)
