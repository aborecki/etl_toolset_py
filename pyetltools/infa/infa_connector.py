import functools
from typing import List

from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector
from pyetltools.infa.infa_cmd_connector import InfaCmdConnector


class InfaConnector(Connector):
    class Sqls:
        sql_connections = """
                    select *
                    from
                    [dbo].[V_IME_CONNECTION]
        """

        sql_labels = """
                    select *
                    from
                    [dbo].[OPB_CM_LABEL]
        """

        def sql_label_max(suffix):
            return f"""
                    select top 1 label_name, last_saved, created_by
                    from
                    [dbo].[OPB_CM_LABEL] where LABEL_NAME like '{suffix}%'
                    order by cast(label_id as datetime) desc 
        """

        sql_queries = """
                SELECT * 
          FROM [D00000TD10_PWC_REP_DEV].[dbo].[OPB_CM_QUERY]
        """

        def sql_query_max(suffix):
            return f"""
                     select top 1 query_name, last_saved, created_by
                     from
                     [dbo].[OPB_CM_QUERY] where QUERY_NAME like '{suffix}%'
                     order by query_id desc 
         """

    def __init__(self, key, infa_repo_db_connector, infa_cmd_connector):
        super().__init__(key)
        self.infa_repo_db_connector_key = infa_repo_db_connector
        self.infa_cmd_connector_key = infa_cmd_connector

    def validate_config(self):
        super().validate_config()
        connector.get(self.infa_cmd_connector_key)
        connector.get(self.infa_repo_db_connector_key)

    def get_infa_repo_db_connector(self):
        return connector.get(self.infa_repo_db_connector_key)

    def get_infa_cmd_connector(self):
        return connector.get(self.infa_cmd_connector_key)

    def get_connections_as_pd_df(self):
        return self.get_infa_repo_db_connector().run_query_pandas_dataframe(self.Sqls.sql_connections)

    def get_connections_as_spark_df(self):
        return self.get_infa_repo_db_connector().run_query_spark_dataframe(self.Sqls.sql_connections)

    def get_labels_as_pd_df(self):
        return self.get_infa_repo_db_connector().run_query_pandas_dataframe(self.Sqls.sql_labels)

    def get_labels(self):
        return sorted(self.get_labels_as_pd_df()["LABEL_NAME"])

    def get_last_created_label(self, label_prefix):
        return self.get_infa_repo_db_connector().run_query_pandas_dataframe(self.Sqls.sql_label_max(label_prefix))

    def check_label_exists(self, label):
        current_labels = [i.upper() for i in self.get_labels()]
        if label.upper() in current_labels:
            return True
        return False

    def create_label(self, label, comment=None):
        infa_cmd = self.get_infa_cmd_connector()

        if self.check_label_exists(label):
            print(f"Label {label} already exists.")
            return False
        infa_cmd.run_pmrep_create_label(label, comment)

        return True

    def execute_query(self, query_name):
        infa_cmd = self.get_infa_cmd_connector()

        if not self.check_query_exists(query_name):
            print(f"Query {query_name} does not exists.")
            return False
        infa_cmd.run_pmrep_execute_query(query_name)

        return True

    def check_query_exists(self, query):
        current_queries = [i.upper() for i in self.get_queries()]
        if query.upper() in current_queries:
            return True
        return False

    def get_queries_as_pd_df(self):
        return self.get_infa_repo_db_connector().run_query_pandas_dataframe(self.Sqls.sql_queries)

    def get_queries(self):
        return sorted(self.get_queries_as_pd_df()["QUERY_NAME"])

    def get_last_created_query(self, label_prefix):
        return self.get_infa_repo_db_connector().run_query_pandas_dataframe(self.Sqls.sql_query_max(label_prefix))

    def create_deployment_query(self, label):

        query_name = label
        if not self.check_label_exists(label):
            print(f"Label {label} not exists.")
            return False
        if self.check_query_exists(query_name):
            print(f"Query {query_name} already exists.")
            return False
        deployment_expression = f"""Label Equals {label} and ReusableStatus In (Non-reusable,Reusable) and LatestStatus in ('Latest Checked-in', Older) and IncludeChildren Where (Any) depends on ((Any), ('Non-reusable Dependency'))"""
        infa_cmd = self.get_infa_cmd_connector()
        infa_cmd.run_pmrep_create_query(query_name, "shared", deployment_expression)
        return True
