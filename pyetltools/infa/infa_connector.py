import functools
from typing import List

from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector
from pyetltools.infa.infa_cmd_connector import InfaCmdConnector





class InfaConnector(Connector):
    def __init__(self, key=None, db_connectors=None, infa_cmd_connectors=None):
        assert db_connectors, "db_connectors InfaConfig parameter cannot be None"
        assert infa_cmd_connectors, "infa_cmd_connectors InfaConfig parameter cannot be None"
        self.db_connectors=None
        super().__init__(key=key)
        if db_connectors:
            self.db_connectors=db_connectors
        if infa_cmd_connectors:
            self.infa_cmd_connectors=infa_cmd_connectors

    def validate_config(self):
        for dc in self.db_connectors:
            assert "db_connector" in self.db_connectors[
                dc], f"{dc} config entry in db_connectors config does not contain db_connector key"
            assert "data_source" in self.db_connectors[
                dc], f"{dc} config entry in db_connectors config does not contain data_source key"
            connector.get( self.db_connectors[dc]) #try to get connector

        for con in self.infa_cmd_connectors:
            connector.get( self.infa_cmd_connectors[con]) #try to get connector

    def get_db_connector_for_env(self, env):
        assert env in self.config.db_connectors, f"Db connector for environemnt {env} not found "
        db_connector_config=self.config.db_connectors[env]
        return connector.get(db_connector_config["db_connector"]).clone_set_data_source(db_connector_config["data_source"])

    def get_infa_cmd_connector_for_env(self, env)-> InfaCmdConnector:
        assert env in self.config.infa_cmd_connectors, f"Infa cmd connector for environemnt {env} not found "
        return connector.get(self.config.infa_cmd_connectors[env])

    class Sqls:
        sql_connections="""
                    select *
                    from
                    [dbo].[V_IME_CONNECTION]
        """

        sql_labels="""
                    select *
                    from
                    [dbo].[OPB_CM_LABEL]
        """


        def sql_label_max(suffix):
                    return f"""
                    select top 1 label_name, creation_time, created_by
                    from
                    [dbo].[OPB_CM_LABEL] where LABEL_NAME like '{suffix}%'
                    order by cast(CREATION_TIME as datetime) desc 
        """

        sql_queries="""
                SELECT * 
          FROM [D00000TD10_PWC_REP_DEV].[dbo].[OPB_CM_QUERY]
        """

        def sql_query_max(suffix):
            return f"""
                     select top 1 query_name, creation_time, created_by
                     from
                     [dbo].[OPB_CM_QUERY] where QUERY_NAME like '{suffix}%'
                     order by cast(CREATION_TIME as datetime) desc 
         """

    def get_connections_as_pd_df(self, envs):
        return functools.reduce(lambda df1,df2 : df1.append(df2),
                                [self.get_db_connector_for_env(env).run_query_pandas_dataframe(self.sql_connections) for env in envs ] )

    def get_connections_as_spark_df(self, envs):
        return functools.reduce(lambda df1,df2 : df1.unionAll(df2),
                                [self.get_db_connector_for_env(env).run_query_spark_dataframe(self.sql_connections) for env in envs ] )

    def get_labels_as_pd_df(self, env):
        return self.get_db_connector_for_env(env).run_query_pandas_dataframe(self.Sqls.sql_labels)

    def get_labels(self, env):
        return sorted(self.get_labels_as_pd_df(env)["LABEL_NAME"])

    def get_max_label_by_creation_date(self, env, label_prefix):
        return self.get_db_connector_for_env(env).run_query_pandas_dataframe(self.Sqls.sql_label_max(label_prefix))

    def check_label_exists(self,env, label):
        current_labels=[i.upper() for i in self.get_labels(env)]
        if label.upper() in current_labels:
            return True
        return False

    def create_label(self, env, label, comment=None):
        infa_cmd=self.get_infa_cmd_connector_for_env(env)

        if self.check_label_exists(env, label):
            print(f"Label {label} already exists.")
            return False
        infa_cmd.run_pmrep_create_label(label, comment)

        return True

    def check_query_exists(self,env, query):
        current_queries=[i.upper() for i in self.get_queries(env)]
        if query.upper() in current_queries:
            return True
        return False

    def get_queries_as_pd_df(self, env):
        return self.get_db_connector_for_env(env).run_query_pandas_dataframe(self.Sqls.sql_queries)

    def get_queries(self, env):
        return sorted(self.get_queries_as_pd_df(env)["QUERY_NAME"])

    def get_max_query_by_creation_date(self, env, label_prefix):
        return self.get_db_connector_for_env(env).run_query_pandas_dataframe(self.Sqls.sql_query_max(label_prefix))



    def create_deployment_query(self, env, label):

        query_name=label
        if not self.check_label_exists(env, label):
            print(f"Label {label} not exists.")
            return False
        if self.check_query_exists(env, query_name):
            print(f"Query {query_name} already exists.")
            return False
        deployment_expression=f"""Label Equals {label} and ReusableStatus In (Non-reusable,Reusable) and LatestStatus in ('Latest Checked-in', Older) and IncludeChildren Where (Any) depends on ((Any), ('Non-reusable Dependency'))"""
        infa_cmd=self.get_infa_cmd_connector_for_env(env)
        infa_cmd.run_pmrep_create_query(query_name,"shared", deployment_expression )
        return True

