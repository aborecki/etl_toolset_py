import bectools
import pyetltools
import bectools.bec.data.sql.recon as sql_recon
import bectools.bec.data.sql.meta as sql_meta
from bectools.bec.model.entities import *

from pyetltools.core import connector
from pyetltools.data.db_connector import DBConnector


class BECDBConnector(DBConnector):

    def validate_config(self):
        super().validate_config()

    sql=bectools.bec.data.sql

    def exec_temp_access(self, group="", comment=""):
        print(self.execute_statement(f"exec sp_tempaccess('{group}','{comment}')"))

    def dr_prod_meta_dr_restore_log(self, table_name, date):
        sql = sql_meta.prod_meta_dr_restore_log(date)
        print("SQL:\n" + sql)
        return self.execute_statement(sql)

    def sqlerver_wflow_run_inst(self, wf_run_id):
        sql = self.sql.monitoring.sqlerver_wflow_run_inst(wf_run_id)
        print("SQL:\n" + sql)
        return self.run_query_pandas_dataframe(sql)




