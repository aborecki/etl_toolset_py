import functools
import re
from typing import List

import pandas

from pyetltools import get_default_logger
from pyetltools.core import connector
from pyetltools.core.connector import Connector
from pyetltools.data.db_connector import DBConnector
from pyetltools.infa.infa_cmd_connector import InfaCmdConnector
from pyetltools.infa.lib import infa_repo
from pyetltools.infa.lib.infa_repo_sqls import get_obp_workflow

from pyetltools.infa.lib.pv import get_persistence_values, get_persistence_values_hist


class InfaConnector(Connector):
    class Sqls:
        sql_connections = """
                    select *
                    from
                    [dbo].[V_IME_CONNECTION]
        """

        sql_connections_v_ime="""
        select  ' ' "REPOSITORY_ID", 
		'com.informatica.powercenter.deployment.'+REPLACE(otype.OBJECT_TYPE_NAME,' ','') "CLASS_ID", 
        otype.OBJECT_TYPE_NAME + '_' + ltrim(str(cnx.OBJECT_ID)) "CONNECTION_ID", 
	'' "CATALOG_ID",
	'' "DATA_MGR_CLASS_ID",
	'' "DATA_MANAGER_ID",
	'' "SCHEMA_CLASS_ID",
	'' "SCHEMA_ID",
	otype.OBJECT_TYPE_NAME "CONNECTION_TYPE", 
	cnx.OBJECT_NAME "CONNECTION_NAME", 
	cnx.COMMENTS "CONNECTION_DESC", 
	mmd_cnx.CNX_SUBTYPE_NAME "CONNECTION_SUBTYPE", 
	cnx.CONNECT_STRING "HOST_NAME", '' "SCHEMA_NAME",
        cnx.USER_NAME "USER_NAME", '' "ADDRESS",
        '' "CONNECTION_TEXT1", '' "CONNECTION_TEXT2",
        '' "CONNECTION_TEXT3", '1' "VERSION_NUM",
        '' "SRC_CREATE_DT", '' "SRC_UPDATE_DT",
        '' "EFF_FROM_DT", '' "EFF_TO_DT"
    from OPB_CNX cnx, OPB_OBJECT_TYPE otype, OPB_REPOSIT repo, OPB_MMD_CNX mmd_cnx
	where otype.OBJECT_TYPE_ID in (73, 74, 75, 76, 77) and repo.RECID = 1 and
              otype.OBJECT_TYPE_ID = cnx.OBJECT_TYPE and
              cnx.OBJECT_TYPE = mmd_cnx.CNX_OBJECT_TYPE and
              cnx.OBJECT_SUBTYPE = mmd_cnx.CNX_OBJECT_SUBTYPE and
              repo.RECID = 1
        """


        sql_connections_with_server_name="""
        Select 
        a.[OBJECT_NAME] as [CONNECTION_NAME],
		c.[Object_type_name] as [CONNECTION_TYPE],
		d.CNX_SUBTYPE_NAME as [CONNECTION_SUBTYPE], 
        a.[USER_NAME], 
        a.CONNECT_STRING,
		a.CODE_PAGE,
		a.COMMENTS,
		CASE a.OBJECT_SUBTYPE
			WHEN '104' THEN (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '10') 
			ELSE NULL
		END as [DATABASE_NAME],
		CASE a.OBJECT_SUBTYPE
			WHEN '104' THEN (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '11')
			ELSE NULL
		END as [DATABASE_SERVER_NAME],
		CASE a.OBJECT_SUBTYPE
			WHEN '104' THEN (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '12')
			ELSE NULL
		END	as [DOMAIN_NAME],
			(Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '13') as [PACKET_SIZE],
			(Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '14') as [TRUSTED_CONNECTION],				
		CASE a.OBJECT_SUBTYPE
			WHEN '104' THEN (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '15')
			ELSE (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '10')
		END	as [CONNECTION_ENVIRONMENT],
		CASE a.OBJECT_SUBTYPE
			WHEN '104' THEN (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '16')
			ELSE (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '11')
		END	as [TRANSACTION_ENVIRONMENT],
		CASE a.OBJECT_SUBTYPE
			WHEN '104' THEN (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '17')
			ELSE (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '12')
		END	as [CONNECTION_RETRY],

		CASE
			WHEN CAST((Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '18') AS varchar) = '1' THEN 'ODBC'
			WHEN CAST((Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '18') AS varchar) = '2' THEN 'OLEDB'
			ELSE (Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '18')
		END as [PROVIDER_TYPE],
		(Select b.ATTR_VALUE from [dbo].[OPB_CNX_ATTR] b where b.OBJECT_ID = a.OBJECT_ID and b.ATTR_ID = '19') as [USE_DSN]
From [dbo].[OPB_CNX] a
Inner join dbo.OPB_OBJECT_TYPE c
on a.[OBJECT_Type] = c.[OBJECT_Type_id]
Inner join dbo.OPB_MMD_CNX d
on a.OBJECT_SUBTYPE = d.CNX_OBJECT_SUBTYPE AND a.[OBJECT_Type] = d.CNX_OBJECT_TYPE
        
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
          FROM [dbo].[OPB_CM_QUERY]
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
        self.logger = get_default_logger()

    def validate_config(self):
        super().validate_config()
        self.env_manager.get_connector_by_key(self.infa_cmd_connector_key)
        connector.get(self.infa_repo_db_connector_key)

    def get_infa_repo_db_connector(self):
        return self.env_manager.get_connector_by_key(self.infa_repo_db_connector_key)

    def get_infa_cmd_connector(self):
        return self.env_manager.get_connector_by_key(self.infa_cmd_connector_key)


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

            l=l.replace(",checkedout", "")
            has_shortcut = ",shortcut" in l
            if has_shortcut:
                 l = l.replace(",shortcut", "")

            if ',' in l and 'Informatica' not in l and l.strip()!="":
                l_s=l.split(",")
                has_reusable=",reusable," in l or ",non-reusable," in l
                has_deleted=",deleted" in l


                if len(l_s) >= 4 or len(l_s)<=6:
                    if has_deleted and has_reusable and len(l_s)==6:
                        objs.append([l_s[0], l_s[1], l_s[2], l_s[3], l_s[4],l_s[5], "shortcut" if has_shortcut else ""])
                    elif has_deleted and  len(l_s)==5:
                        objs.append([l_s[0], l_s[1],"", l_s[2], l_s[3], l_s[4], "shortcut" if has_shortcut else ""])
                    elif has_reusable and  len(l_s)==5:
                        objs.append([l_s[0], l_s[1], l_s[2], l_s[3], l_s[4],"", "shortcut" if has_shortcut else ""])
                    elif  len(l_s)==4:
                        objs.append([l_s[0], l_s[1],"", l_s[2], l_s[3],"", "shortcut" if has_shortcut else ""])
                    else:
                        raise Exception("Cannot parse: "+l)
        return pandas.DataFrame(objs, columns=["FOLDER","OBJECT_TYPE","REUSABLE","OBJECT_NAME","VERSION_NUMBER","DELETED","SHORTCUT"])

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


    def get_sources(self):
        sql="""
            select * from dbo.rep_all_sources
        """
        self.logger.debug(sql)
        return self.infa_repo_db_connector_key.query_pandas(sql)

    def get_targets(self, s):
        sql="""
            select * from dbo.rep_all_targets
        """
        self.logger.debug(sql)
        return self.infa_repo_db_connector_key.query_pandas(sql)


    def get_persistence_values(self, workflow_name):
        return get_persistence_values(self.infa_repo_db_connector_key, workflow_name)

    def get_persistence_values_file(self, workflow_name):
        return "Folder;Workflow;Session;PersistentVariable;Value;Runinst_name\n"+"\n".join(list(self.get_persistence_values(workflow_name)["PV_FILE"]))

    def get_persistence_values_hist(self, workflow_name):
        return get_persistence_values_hist(self.infa_repo_db_connector_key, workflow_name)

    def get_wf_metadata_extension_values(self):
        return infa_repo.get_wf_metadata_extension_values(self.infa_repo_db_connector_key)

    def get_metadata_extension_values(self):
        return infa_repo.get_metadata_extension_values(self.infa_repo_db_connector_key)

    def get_connections(self):
        return self.get_infa_repo_db_connector().query_pandas(self.Sqls.sql_connections_with_server_name)

    def get_workflows(self):
        return self.get_infa_repo_db_connector().query_pandas(get_obp_workflow())