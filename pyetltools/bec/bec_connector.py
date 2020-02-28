import os
import re
from functools import reduce

from pyetltools.bec.data.sql import recon
from pyetltools.bec.hgrepo.hgprod_conector import HGProdConnector
from pyetltools.bec.jenkins.bec_jenkins_connector import BECJenkinsConnector
from pyetltools.bec.model.entities import ReleaseStatusWorkflow, ReleaseStatusTable
from pyetltools.core import connector
from pyetltools.core.cmd import Cmd
from pyetltools.core.connector import Connector
import pyetltools.bec.data.sql.meta as sql_meta
import pyetltools.bec.data.sql.recon as sql_recon
from pyetltools.data.db_connector import DBConnector
from pyetltools.infa.infa_cmd_connector import InfaCmdConnector
from pyetltools.infa.infa_connector import InfaConnector
from pyetltools.jenkins.jenkins_connector import JenkinsConnector


def input_YN( prompt):
    ans = None
    while ans not in ['Y', 'N']:
        ans = input(prompt).upper()
    return ans == 'Y'
    return ans == 'Y'

class BECConnector(Connector):

    def __init__(self, key, infa_repo_db_connectors={}, infa_connectors={}, hgprod_connectors={}, jenkins_connectors={}, hg_repos_path=None):
        super().__init__(key)
        self.hg_repos_path=hg_repos_path
        self.env_manager = BECConnector.EnvManager(infa_repo_db_connectors, infa_connectors, hgprod_connectors, jenkins_connectors)
        self.recon = self.Recon(self)
        self.deployment = self.Deployment(self)

    def validate_config(self):
        super().validate_config()
        self.env_manager.validate_config()

    class EnvManager:

        def __init__(self, infa_repo_db_connectors: dict = None, infa_connectors: dict = None,
                     hgprod_connectors: dict = None, jenkins_connectors:dict = None, ):
            self.infa_repo_db_connectors = infa_repo_db_connectors
            self.infa_connectors = infa_connectors
            self.hgprod_connectors = hgprod_connectors
            self.jenkins_connectors=jenkins_connectors

        def validate_config(self):
            for conn in self.infa_repo_db_connectors.values():
                connector.get(conn)  # try to get connector
            for conn in self.infa_connectors.values():
                connector.get(conn)  # try to get connector
            for conn in self.hgprod_connectors.values():
                connector.get(conn)  # try to get connector
            for conn in self.jenkins_connectors.values():
                connector.get(conn)  # try to get connector

        def get_db_connector_for_env(self, env) -> DBConnector:
            assert env in self.infa_repo_db_connectors, f"Infa repo DB connector for environment {env} not found "
            return connector.get(self.infa_repo_db_connectors[env])

        def get_infa_connector_for_env(self, env) -> InfaConnector:
            assert env in self.infa_connectors, f"Infa connector for environment {env} not found "
            return connector.get(self.infa_connectors[env])

        def get_hgprod_connector_for_env(self, env) -> HGProdConnector:
            assert env in self.hgprod_connectors, f"HGProd connector for environment {env} not found "
            return connector.get(self.hgprod_connectors[env])

        def get_jenkins_connector_for_env(self, env) -> BECJenkinsConnector:
            assert env in self.jenkins_connectors, f"Jenkins connector for environment {env} not found "
            return connector.get(self.jenkins_connectors[env])

    class Recon:
        def __init__(self, connector):
            self.parent_connector=connector

        def start_recon(self, date):
            self.parent_connector.env_manager.get_jenkins_connector_for_env("FTST2").bec_start_recon(date)

        def start_push_recon(self, date):
            self.parent_connector.env_manager.get_jenkins_connector_for_env("FTST2").bec_start_push_recon(date)


    class Deployment:
        def __init__(self, connector):
            self.parent_connector=connector

        def hg_deploy_file_set_label(self, path, label, modify_cloning=False, clonevar=True, clonelist=True):
            with open(path, 'r') as f:
                content = f.read()
            content_new = re.sub(r'^\s*LABEL\s*=\s*.*$', r'LABEL='+label, content, count=1, flags=re.IGNORECASE|re.M)

            if modify_cloning:
                if not clonevar:
                    content_new = re.sub(r'^\s*CLONEVAR', r'#CLONEVAR', content_new,
                                         flags=re.IGNORECASE | re.M)
                else:
                    # uncomment first occurence of CLONEVAR if commented out
                    content_new = re.sub(r'^\s*(#\s*)+CLONEVAR', r'CLONEVAR', content_new, count=1,
                                             flags=re.IGNORECASE | re.M)
                if not clonelist:
                    content_new = re.sub(r'^\s*CLONELIST', r'#CLONELIST', content_new,
                                         flags=re.IGNORECASE | re.M)
                else:
                    content_new = re.sub(r'^\s*(#\s*)+CLONELIST', r'CLONELIST', content_new, count=1,
                                         flags=re.IGNORECASE | re.M)
            with open(path, 'w') as f:
                f.write(content_new)

        def add_tag_to_commit(self, repository, changeset_id, tag):
            hgtags_file = os.path.join(self.parent_connector.hg_repos_path, repository, ".hgtags")
            print(f"Adding tag {tag} to {changeset_id}  in "+hgtags_file)
            #write the new line to the end - I used append to avoid checking for newline at the end of exisintg file
            with open(hgtags_file, 'r') as f:
                current_content=f.readlines()
            newline=f"{changeset_id} {tag}"

            if f"{changeset_id} {tag}" in [l.strip() for l in current_content]:
                print(f"{newline} already in file {hgtags_file}")
            else:
                with open(hgtags_file, 'w', newline="\n") as f:
                    current_content.append(f"{newline}\n")
                    f.writelines(current_content)

        def hg_get_tags(self, hg_command):
            out, _,_=hg_command.run("tags","--debug")
            lines=out.splitlines()
            ret=[]
            for l in lines:
                fields=reduce(lambda a,b: a+b,[ d.split(":").split() for d in lines])
                ret.append(fields[0], fields[2])
            return ret

        def hg_deploy_file_change_commit(self, repository, label, tag):

            repository_dir=os.path.join(self.parent_connector.hg_repos_path, repository)
            hg_command = Cmd("hg", working_dir=repository_dir)
            hg_command.run("pull")

            modify_cloning=input_YN("Do you want to change clonevar/clonelist variables?")
            use_clonevar=use_clonelist=None
            if modify_cloning:
                use_clonevar=input_YN("Do you want to use clonevar?")
                use_clonelist=input_YN("Do you want to use clonelist?")

            deploy_file_path=os.path.join(repository_dir,"PWC_deploy.txt")

            self.hg_deploy_file_set_label(deploy_file_path, label,
                                          modify_cloning=modify_cloning, clonevar=use_clonevar, clonelist=use_clonelist)
            print(f"{deploy_file_path} modified.")
            Cmd("hg", working_dir=repository_dir).run("diff", "PWC_deploy.txt")

            if not input_YN(f"Do you want to commit changes to PWC_deploy.txt (you can go now and edit {deploy_file_path} file manually)"):
                   return False


            hg_command.run("pull")
            hg_command.run("add", "PWC_deploy.txt")
            hg_command.run("commit", "-m",label, "PWC_deploy.txt")
            id_out,_,_= hg_command.run("id","-i", "--debug")
            changeset_id=id_out.strip().rstrip("+")
            print("Last changeset"+changeset_id)
            self.add_tag_to_commit(repository, changeset_id, tag)
            hg_command.run("diff", ".hgtags")
            if not input_YN(f"Do you want to commit changes to .hgtags"):
                return False
            hg_command.run("commit", "-m", f"Added tag {tag} for changeset ", ".hgtags")

            hg_command.run("outgoing", "-v")
            if not input_YN(f"Do you want to push above changes?"):
                return False
            hg_command.run("push")

        def deploy_infa_by_label(self, target_env, label, confirm_all=False):
            infa_connector =  self.parent_connector.env_manager.get_infa_connector_for_env("DEV")
            hgprod_connector = self.parent_connector.env_manager.get_hgprod_connector_for_env(target_env)
            jenkins_connector = self.parent_connector.env_manager.get_jenkins_connector_for_env(target_env)

            assert hgprod_connector.environment, f"HGProd connector for env {target_env} has not environment configured."

            if not infa_connector.check_label_exists(label):
                print(f"Label {label} does not exist.")
                if confirm_all or input_YN(f"Do you want to create label {label}?"):
                    infa_connector.create_label(label)
            else:
                print(f"Label {label} exists.")
                if confirm_all or input_YN(f"Do you want to re-deploy label {label} to {target_env}?") :
                    pass
                else:
                    print("Done.")
                    return False
            # Create deployment query

            if not infa_connector.check_query_exists(label):
                print(f"Depoyment query {label} does not exist.")
                if confirm_all or input_YN(f"Do you want to create deployment query {label}?") :
                    infa_connector.create_deployment_query(label)
                else:
                    print("Nothing to do. Done.")
                    return False
            else:
                print(f"Deployment query {label} exists.")
                if confirm_all or input_YN(f"Do you want to reuse query {label} to deploy to {target_env}?") :
                    pass
                else:
                    print("Nothing to do. Done.")
                    return False
            print("Verifying if all objects are checked in")
            checkedout_obj_df = infa_connector.find_checkout_as_pandas_df()
            labeled_obj=infa_connector.execute_query_as_pandas_df(label)

            checkedout_labeled_df=labeled_obj.merge(checkedout_obj_df, on=["FOLDER","OBJECT_NAME"])

            if len(checkedout_labeled_df)>0:
                print("Some labeled objects are not checked in:")
                print(checkedout_labeled_df)
                if input_YN("Do you want to continue?"):
                    pass
                else:
                    return False
            else:
                print("All objects are checked in.")





            print(labeled_obj)
            if confirm_all or input_YN(f"Do you want to deploy above objects?") :
                pass
            else:
                print("Done.")
                return False

            area= label.split("_")[0] #MDW, RAP
            repo = "PWC_" +area
            tag = "PWC_" + label
            #Hg repos

            if input_YN(f"Do you want to change PWC_deploy.txt file"):
                self.hg_deploy_file_change_commit(repo, label, tag)


            # Bestil


            jenkins_area=area # MDW, RAP etc

            if confirm_all or input_YN(f"Do you want to bestil tag {tag} on hgprod environment "
                                 f"{hgprod_connector.environment} and hgprod repository {repo}?") :
                pass
            else:
                print("Done.")
                return False;


            hgprod_connector.bestil(repo, tag)

            if confirm_all or input_YN(f"Do you want to start jenkins build for environment {jenkins_connector.environment} and area {jenkins_area}?"):
                pass
            else:
                print("Done.")
                return False;


            jenkins_connector.bestil(jenkins_area)

    # def get_ftst2_db_connector(self):
    #     return  connector.get("DB/NZFTST2").set_data_source("MMDB")

    def get_bank_koncern_map(self):
        sql= sql_meta.sql_ftst2_bank_koncern()
        print("SQL:\n"+sql)
        return self.env_manager.env_manager.get_db_connector_for_env("FTST2").run_query_spark_dataframe(sql)

    def recon_add_aggregate_foreign_key(self, table_name, column_name):
         sql = sql_recon.ftst2_insert_aggregate_foreign_key(table_name, column_name)
         print("SQL:\n"+sql)
         return self.env_manager.get_db_connector_for_env("FTST2").execute_statement(sql)
    #
    def recon_remove_aggregated_results_for_table_name(self, table_name, date):
         sql = recon.ftst2_delete_recon_results_for_table(table_name, date)
         print("SQL:\n" + sql)
         return connector.get("DB/NZFTST2").clone().set_data_source("MMDB").execute_statement(sql)
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
