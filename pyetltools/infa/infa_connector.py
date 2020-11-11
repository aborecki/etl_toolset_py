import functools
import re
from typing import List

import pandas

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

    def __init__(self, key=None, infa_repo_db_connector=None, infa_cmd_connector=None):
        super().__init__(key)
        self.infa_repo_db_connector_key = infa_repo_db_connector
        self.infa_cmd_connector_key = infa_cmd_connector

    def validate_config(self):
        super().validate_config()
        self.env_manager.get_connector_by_key(self.infa_cmd_connector_key)
        connector.get(self.infa_repo_db_connector_key)

    def get_infa_repo_db_connector(self):
        return self.env_manager.get_connector_by_key(self.infa_repo_db_connector_key)

    def get_infa_cmd_connector(self):
        return self.env_manager.get_connector_by_key(self.infa_cmd_connector_key)

    def get_connections_as_pd_df(self):
        return self.get_infa_repo_db_connector().query_pandas(self.Sqls.sql_connections)

    def get_connections_as_spark_df(self):
        return self.get_infa_repo_db_connector().query_spark(self.Sqls.sql_connections)

    def get_labels_as_pd_df(self):
        return self.get_infa_repo_db_connector().query_pandas(self.Sqls.sql_labels)

    def get_labels(self):
        return sorted(self.get_labels_as_pd_df()["LABEL_NAME"])

    def get_last_created_label(self, label_prefix):
        return self.get_infa_repo_db_connector().query_pandas(self.Sqls.sql_label_max(label_prefix))

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

    def execute_query(self, query_name,sep=" "):
        if not self.check_query_exists(query_name):
            print(f"Query {query_name} does not exists.")
            return None
        return self.get_infa_cmd_connector().run_pmrep_execute_query(query_name, sep=sep)

    def execute_query_as_pandas_df(self, query_name):
        ret= self.execute_query(query_name, sep=",")
        if ret.returncode == 0:
            return self._parse_query_output_as_pandas_df(ret)


    def find_checkout(self, sep=" "):
         return self.get_infa_cmd_connector().run_pmrep_find_checkout(sep=sep)

    def find_checkout_as_pandas_df(self):
        ret=self.find_checkout(sep=",")
        if ret.returncode == 0:
            return self._parse_query_output_as_pandas_df(ret)
        else:
            return None

    def _parse_query_output_as_pandas_df(self, output):
        objs = []
        lines=output.stdout.decode("latin-1").splitlines()
        for line in lines:
            l=line.strip()
            if ',' in l and 'Informatica' not in l and l.strip()!="":
                m = re.match(r"([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)", l)
                if m:
                    objs.append((m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)))
                else:
                    m = re.match(r"([^,]+),([^,]+),([^,]+),([^,]+)", l)
                    if m:
                        objs.append((m.group(1), m.group(2), "", m.group(3), m.group(4)))
                    else:
                        raise Exception("Cannot parse"+l)
        return pandas.DataFrame(objs, columns=["FOLDER","OBJECT_TYPE","REUSABLE","OBJECT_NAME","VERSION_NUMBER"])

    def delete_query(self, query_name, query_type):
        infa_cmd = self.get_infa_cmd_connector()

        if not self.check_query_exists(query_name):
            print(f"Query {query_name} does not exists.")
            return False
        infa_cmd.run_pmrep_delete_query(query_name, query_type)

        return True

    def check_query_exists(self, query):
        current_queries = [i.upper() for i in self.get_queries()]
        if query.upper() in current_queries:
            return True
        return False

    def get_queries_as_pd_df(self):
        return self.get_infa_repo_db_connector().query_pandas(self.Sqls.sql_queries)

    def get_queries(self):
        return sorted(self.get_queries_as_pd_df()["QUERY_NAME"])

    def get_last_created_query(self, label_prefix):
        return self.get_infa_repo_db_connector().query_pandas(self.Sqls.sql_query_max(label_prefix))


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


    def create_deployment_query_checked_out(self, label, query_name, query_type="shared"):
        if not self.check_label_exists(label):
            print(f"Label {label} not exists.")
            return False
        if self.check_query_exists(query_name):
            print(f"Query {query_name} already exists.")
            return False
        deployment_expression = f"""LatestStatus in (Checked-out)  """
        infa_cmd = self.get_infa_cmd_connector()
        infa_cmd.run_pmrep_create_query(query_name, query_type, deployment_expression)
        return True
