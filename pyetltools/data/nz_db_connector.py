from pyetltools import get_default_logger
from pyetltools.core.env_manager import get_env_manager
from pyetltools_bec.data.bec_db_connector import BECDBConnector
import tempfile
import os
from pathlib import Path

class NZDBConnector(BECDBConnector):
    def exec_temp_access(self, group="", comment=""):
        print(self.execute_statement(f"exec sp_tempaccess('{group}','{comment}')"))


    def get_group_env_name(self, env):
        env_name = env.value;
        if "FTST" in env_name:
                env_name="FTST"
        if "PROD" in env_name:
                env_name="PROD"
        return env_name

    def exec_temp_access_read(self, comment=""):

        env=get_env_manager().get_environment_for_connector(self)

        env_name = self.get_group_env_name(env)

        self.exec_temp_access(f"UG_{env_name}_DBO_R", comment)

    def exec_temp_access_admin(self, comment=""):

        env=get_env_manager().get_environment_for_connector(self)

        env_name = self.get_group_env_name(env)

        self.exec_temp_access(f"UG_{env_name}_DBO_A", comment)

    def exec_temp_access_meta_admin(self, comment=""):

        env=get_env_manager().get_environment_for_connector(self)

        env_name = self.get_group_env_name(env)

        self.exec_temp_access(f"UG_{env_name}_META_A", comment)

    def insert_df_to_table(self, df, target_table, truncate_table=False, temp_file_dir=None, null_value="?"):
        trunc_stmt = ""

        if truncate_table:
            trunc_stmt = f"truncate table {target_table};\n"

        if temp_file_dir is None:
            temp_file_dir = os.path.join(tempfile.gettempdir(), "PYETLTOOLS_TEMP_FILES")

        Path(temp_file_dir).mkdir(parents=True, exist_ok=True)

        df = df.fillna(null_value)
        filename = temp_file_dir + f"\\{target_table}.csv"
        df.to_csv(filename, index=False)

        get_default_logger().debug("Saving temp file as " + str(filename))

        sql = f"""
            {trunc_stmt}
            insert into {target_table} select * from 
            EXTERNAL '{temp_file_dir}\\{target_table}.csv'
            sameas {target_table}
            USING
            (
                DELIMITER ','
                REMOTESOURCE 'ODBC'
                ESCAPECHAR '\\'
                LogDir '{temp_file_dir}'
                skiprows 1
                NullValue '{null_value}'
            ); commit;
            """
        get_default_logger().debug("SQL:"+sql)
        self.execute_statement(sql)





