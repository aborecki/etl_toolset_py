from pyetltools import get_default_logger
import tempfile
import os
from pathlib import Path

from pyetltools.data.cached_db_connector import CachedDBConnector
from pyetltools.data.db_connector import DBConnector


class NZDBConnector(DBConnector):

    def insert_df_to_table(self, df, target_table, truncate_table=False, temp_file_dir=None, null_value="?"):
        trunc_stmt = ""

        if truncate_table:
            trunc_stmt = f"truncate table {target_table};\n"

        if temp_file_dir is None:
            temp_file_dir = os.path.join(tempfile.gettempdir(), "PYETLTOOLS_TEMP_FILES")

        Path(temp_file_dir).mkdir(parents=True, exist_ok=True)

        df = df.fillna(null_value)
        df = df.replace(r'\\', r'\\\\',regex=True)
        df=df.replace(',', r'\,', regex=True)
        df = df.replace('\n', '\\\n', regex=True)
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
                QuotedValue 'Double'
            ); commit;
            """
        get_default_logger().debug("SQL:"+sql)
        self.execute_statement(sql)





