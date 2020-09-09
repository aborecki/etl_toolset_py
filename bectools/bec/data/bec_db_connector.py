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
        return self.query_pandas(sql)


    def query_pandas_cache(self, query, force_reload_from_source=False):
        import hashlib
        return bectools.connectors.get("CACHE").get_from_cache("TEMP_QUERY_CACHE_"+self.key +
                                                               "_"+self.data_source +"_" +query[0:50]+"_"+
                                                               str(hashlib.md5(query.encode('utf-8')).hexdigest()),
                                                               lambda : self.query_pandas(query),force_reload_from_source)
