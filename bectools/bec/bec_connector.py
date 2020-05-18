import os
import re
from functools import reduce
import winsound


from bectools.bec.data.sql import recon
from bectools.bec.enums import ResourceType, Env
from pyetltools.core import connector
from pyetltools.core.cmd import Cmd
from pyetltools.core.connector import Connector
import bectools.bec.data.sql.meta as sql_meta
import bectools.bec.data.sql.recon as sql_recon
from pyetltools.core.env import EnvManager

def input_YN( prompt):
    ans = None
    while ans not in ['Y', 'N']:
        ans = input(prompt).upper()
    return ans == 'Y'






class BECConnector(Connector):

    def __init__(self, key, env_manager: EnvManager, hg_repository_path=None):
        super().__init__(key)
        self.hg_repos_path=hg_repository_path
        self.env_manager = env_manager
        self.recon = self.Recon(self)
        self.deployment = self.Deployment(self)

    def validate_config(self):
        super().validate_config()
        self.env_manager.validate_config()



    class Recon:
        def __init__(self, connector):
            self.parent_connector=connector

        def start_recon(self, date):
            self.parent_connector.env_manager.get_connector(ResourceType.JENKINS, Env.FTST2).bec_start_recon(date)

        def start_push_recon(self, date):
            self.parent_connector.env_manager.get_connector(ResourceType.JENKINS, Env.FTST2).bec_start_push_recon(date)


    class Deployment:
        def __init__(self, connector):
            self.parent_connector=connector


        def get_last_created_label(self, label_prefix):
            infa_connector = self.parent_connector.env_manager.get_connector(ResourceType.INFA, Env.DEV)
            return infa_connector.get_last_created_label(label_prefix)

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
            id_res= hg_command.run("id","-i", "--debug")
            id_stdout=id_res.stdout.decode("latin-1")
            changeset_id=id_stdout.strip().rstrip("+")
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

        def deploy_infa_by_label(self, target_env, label, confirm_all_safe=False, confirm_do_not_modify_pwc_deploy=False):
            # confirm_all_safe - automatically answer Y to all operations which are "safe"

            target_env=Env[target_env]

            infa_connector =  self.parent_connector.env_manager.get_connector(ResourceType.INFA, Env.DEV)
            hgprod_connector = self.parent_connector.env_manager.get_connector(ResourceType.HGPROD, target_env)
            jenkins_connector = self.parent_connector.env_manager.get_connector(ResourceType.JENKINS,  target_env)

            assert hgprod_connector.environment, f"HGProd connector for env {target_env} has not environment configured."

            if not infa_connector.check_label_exists(label):
                print(f"Label {label} does not exist.")
                if  input_YN(f"Do you want to create label {label}?"):
                    infa_connector.create_label(label)
            else:
                print(f"Label {label} exists.")
                if confirm_all_safe or input_YN(f"Do you want to re-deploy label {label} to {target_env}?") :
                    pass
                else:
                    print("Done.")
                    return False
            # Create deployment query

            if not infa_connector.check_query_exists(label):
                print(f"Depoyment query {label} does not exist.")
                if  input_YN(f"Do you want to create deployment query {label}?") :
                    infa_connector.create_deployment_query(label)
                else:
                    print("Nothing to do. Done.")
                    return False
            else:
                print(f"Deployment query {label} exists.")
                if confirm_all_safe or input_YN(f"Do you want to reuse query {label} to deploy to {target_env}?") :
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
            if confirm_all_safe or input_YN(f"Do you want to deploy above objects?") :
                pass
            else:
                print("Done.")
                return False

            area= label.split("_")[0] #MDW, RAP
            repo = "PWC_" +area
            tag = "PWC_" + label
            #Hg repos

            if not confirm_do_not_modify_pwc_deploy and input_YN(f"Do you want to change PWC_deploy.txt file"):
                self.hg_deploy_file_change_commit(repo, label, tag)


            # Bestil


            jenkins_area=area # MDW, RAP etc

            if confirm_all_safe or input_YN(f"Do you want to bestil tag {tag} on hgprod environment "
                                 f"{hgprod_connector.environment} and hgprod repository {repo}?") :
                pass
            else:
                print("Done.")
                return False;


            hgprod_connector.bestil(repo, tag)

            if confirm_all_safe or input_YN(f"Do you want to start jenkins build for environment {jenkins_connector.environment} and area {jenkins_area}?"):
                pass
            else:
                print("Done.")
                return False;


            ret, a= jenkins_connector.deploy_project(jenkins_area)
            return ret


        def deploy_infa_by_labels(self, target_envs, label, confirm_all_safe=False, confirm_do_not_modify_pwc_deploy=False):

            for env in target_envs:
                ret=self.deploy_infa_by_label(env, label, confirm_all_safe,
                                                          confirm_do_not_modify_pwc_deploy)
                if not ret:
                    self.play_fail_sound()
                    return False

            self.play_success_sound()

        def tone(self, freq, length, msg):
            winsound.Beep(int(min(max(freq, 28), 32766)), int(length))

        def play_success_sound(self):
            self.tone(523, 231.480833333, "Playing Tone: 523 = C5 minus 1 cents")
            self.tone(440, 231.480833333, "Playing Tone: 440 = A4 plus 0 cents")
            self.tone(391, 231.480833333, "Playing Tone: 391 = G4 minus 4 cents")
            self.tone(587, 231.480833333, "Playing Tone: 587 = D5 minus 1 cents")
            self.tone(783, 231.480833333, "Playing Tone: 783 = G5 minus 2 cents")

        def play_fail_sound(self):
            frequency = 2500  # Set Frequency To 2500 Hertz
            duration = 100  # Set Duration To 1000 ms == 1 second
            for i in range(5):
                winsound.Beep(frequency, duration)

    # def get_ftst2_db_connector(self):
    #     return  connector.get("DB/NZFTST2").set_data_source("MMDB")

    def get_bank_koncern_map_pd(self):
        sql= sql_meta.sql_ftst2_bank_koncern()
        print("SQL:\n"+sql)
        return self.env_manager.get_connector(ResourceType.DB,Env.FTST2,"NZ").run_query_pandas_dataframe(sql)

    def recon_add_aggregate_foreign_key(self, table_name, column_name):
         sql = sql_recon.ftst2_insert_aggregate_foreign_key(table_name, column_name)
         print("SQL:\n"+sql)
         return self.env_manager.get_connector(ResourceType.DB,Env.FTST2,"NZ").with_data_source("MMDB").execute_statement(sql)
    #
    def recon_remove_aggregated_results_for_table_name(self, table_name, date):
         sql = recon.ftst2_delete_recon_results_for_table(table_name, date)
         print("SQL:\n" + sql)
         return connector.get("DB/FTST2/NZ").with_data_source("MMDB").execute_statement(sql)
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
