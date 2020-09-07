import os
import re
import winsound


from bectools.bec.data.sql import recon
from bectools.bec.core.env import ResourceType, Env
from pyetltools.core import connector
from pyetltools.core.connector import Connector
import bectools.bec.data.sql.meta as sql_meta
import bectools.bec.data.sql.recon as sql_recon
from pyetltools.core.env import EnvManager
import logging

logger = logging.getLogger("bectools")

def input_YN( prompt):
    ans = None
    while ans not in ['Y', 'N']:
        ans = input(prompt).upper()
    return ans == 'Y'

import functools

class RetryDecorator(object):
    def __init__(self):
        pass
    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            retry=True
            while retry:
                try:
                    result = fn(*args, **kwargs)
                    return result
                except Exception as ex:
                    print("Exception {0}".format(ex))
                    retry=input_YN("Do you want to retry?")
                    if not retry:
                        raise ex
        return decorated

class Recon:
    def __init__(self, connector):
        self.parent_connector=connector

    def start_recon(self, date):
        self.parent_connector.env_manager.get_connector(ResourceType.JENKINS, Env.FTST2).bec_start_recon(date)

    def start_push_recon(self, date):
        self.parent_connector.env_manager.get_connector(ResourceType.JENKINS, Env.FTST2).bec_start_push_recon(date)



class Monitoring:

    def __init__(self, connector):
        self.parent_connector=connector
        self.infa_connector = self.parent_connector.env_manager.get_connector(ResourceType.INFA, Env.DEV)
        self.hg_connector = self.parent_connector.env_manager.get_connector(ResourceType.HG)
        self.infa_connector =  self.parent_connector.env_manager.get_connector(ResourceType.INFA, Env.DEV)

    from collections import namedtuple
    SearchResult = namedtuple("SearchResult", "runs ids")

    def get_search_condition(self, workflow_names, task_name=None, last_days=None, start_time_from=None, start_time_end=None):
        if isinstance(workflow_names, str):
            workflow_names=[workflow_names]
        cond = "("+ " OR ".join([f""" UPPER(WORKFLOW_NAME) LIKE UPPER('{workflow_name}') """ for workflow_name in workflow_names])+")"
        if start_time_from:
            cond += f""" AND START_TIME >'{start_time_from}'"""
        if start_time_end:
            cond += f""" AND START_TIME <'{start_time_end}'"""
        if last_days:
            cond += f""" AND START_TIME > DATEADD(D,-({last_days}-1),CAST(GETDATE() AS DATE))"""
            # --AND START_TIME > DATEADD(D,-{last_days},(SELECT MAX(CAST(START_TIME AS DATE) )  FROM BEC_TASK_INST_RUN WHERE UPPER(WORKFLOW_NAME) LIKE UPPER('%{workflow_name}%')))
        if task_name:
            cond += f""" AND TASK_NAME  LIKE '{task_name}'"""

        return cond

    def search_workflow_task_runs(self, env, workflow_name, task_name=None, last_days=1, start_time_from=None,
                                  start_time_end=None):
        db_con = self.parent_connector.env_manager.get_pc_rep_db_connector(env)

        cond = self.get_search_condition(workflow_name, task_name, last_days, start_time_from, start_time_end)
        query = f"""
            SELECT  DISTINCT CAST(START_TIME AS DATE) DATE_DAY,  WORKFLOW_RUN_ID, WORKFLOW_NAME, RUNINST_NAME, TASK_NAME, MAPPING_NAME, START_TIME,
            END_TIME, 
            CASE WHEN END_TIME IS NULL THEN 'RUNNING' ELSE 'FINISHED' END  RUNNING_STATUS,
            OPC_JOBNAME ,SERVER_NAME ,  SUBJECT_AREA,
            CASE WHEN ISNUMERIC(BANK_KORSEL_NR)=1 THEN LEFT(WORKFLOW_NAME, LEN(WORKFLOW_NAME)-LEN(BANK_KORSEL_NR)-1) ELSE WORKFLOW_NAME END WORKFLOW_NAME_SHORT,
            CASE WHEN ISNUMERIC(BANK_KORSEL_NR)=1 THEN BANK_KORSEL_NR ELSE '' END BANK_KORSEL_NR, LOG_FILE, RUN_ERR_MSG, RUN_STATUS_CODE ,
            TARG_SUCCESS_ROWS,
            SRC_SUCCESS_ROWS
            --, SRCTGT_COUNT 
                FROM (SELECT *, RIGHT(WORKFLOW_NAME, CHARINDEX('_', REVERSE(WORKFLOW_NAME))-1 )  BANK_KORSEL_NR FROM BEC_TASK_INST_RUN WHERE {cond}) x 
            ORDER BY 1,3,2
            """
        print("Search query: " + query)
        runs = db_con.query_pandas(query)
        runs['DATE_DAY'] = runs['DATE_DAY'].astype('datetime64[ns]')
        runs['START_TIME'] = runs['START_TIME'].astype('datetime64[ns]')
        runs['END_TIME'] = runs['END_TIME'].astype('datetime64[ns]')
        ids = set(runs["WORKFLOW_RUN_ID"])
        return Monitoring.SearchResult(runs=runs, ids=ids)

    def search_workflow_runs(self, env, workflow_name, last_days=1, start_time_from=None, start_time_end=None):
        db_con = self.parent_connector.env_manager.get_pc_rep_db_connector(env)
        cond = self.get_search_condition(workflow_name, None, last_days, start_time_from, start_time_end)

        query = f"""
            WITH src as (
            SELECT 
                x.SUBJECT_ID,
                x.WORKFLOW_ID,
                x.WORKFLOW_RUN_ID,
                x.WORKFLOW_NAME,
                (
                            SELECT convert(varchar(100),METADATA_EXTN_VALUE)	
                            FROM  dbo.REP_METADATA_EXTNS AS M
                            WHERE (METADATA_EXTN_NAME = 'OPC_JOBNAME') 
                            AND (x.WORKFLOW_ID = METADATA_EXTN_OBJECT_ID) 
                            AND (x.VERSION_NUMBER = VERSION_NUMBER)
                        ) AS OPC_JOBNAME, 
                x.SERVER_ID,
                x.SERVER_NAME,
                x.START_TIME,
                x.END_TIME,
                x.LOG_FILE,
                x.RUN_ERR_CODE,
                x.RUN_ERR_MSG,
                x.RUN_STATUS_CODE,
                x.USER_NAME,
                x.RUN_TYPE,
                x.VERSION_NUMBER,
                OPB_SUBJECT.SUBJ_NAME SUBJECT_AREA,
                x.RUNINST_NAME
            FROM 
                OPB_WFLOW_RUN x,
                OPB_SUBJECT
            WHERE
                x.SUBJECT_ID = OPB_SUBJECT.SUBJ_ID
            )
            SELECT  CAST(START_TIME AS DATE) DATE_DAY,  WORKFLOW_RUN_ID, WORKFLOW_NAME, RUNINST_NAME, START_TIME, END_TIME, 
            CASE WHEN END_TIME IS NULL THEN 'RUNNING' ELSE 'FINISHED' END  RUNNING_STATUS,
            OPC_JOBNAME ,SERVER_NAME ,  SUBJECT_AREA,
            CASE WHEN ISNUMERIC(BANK_KORSEL_NR)=1 THEN LEFT(WORKFLOW_NAME, LEN(WORKFLOW_NAME)-LEN(BANK_KORSEL_NR)-1) ELSE WORKFLOW_NAME END
            WORKFLOW_NAME_SHORT,
            CASE WHEN ISNUMERIC(BANK_KORSEL_NR)=1 THEN BANK_KORSEL_NR ELSE '' END BANK_KORSEL_NR, RUN_ERR_MSG , RUN_STATUS_CODE
            --, SUM(SRCTGT_COUNT) SUM_SRCTGT_COUNT 
                FROM (SELECT *, RIGHT(WORKFLOW_NAME, CHARINDEX('_', REVERSE(WORKFLOW_NAME))-1 )  BANK_KORSEL_NR 
            FROM src WHERE {cond}) x 
            ORDER BY 1,3,2"""
        logger.debug("Search query: "+query)
        runs = db_con.query_pandas(query)
        runs['DATE_DAY'] = runs['DATE_DAY'].astype('datetime64[ns]')
        runs['START_TIME'] = runs['START_TIME'].astype('datetime64[ns]')
        runs['END_TIME'] = runs['END_TIME'].astype('datetime64[ns]')
        ids = set(runs["WORKFLOW_RUN_ID"])
        return Monitoring.SearchResult(runs=runs, ids=ids)

    def get_jcl_for_opcjob_name(self, opc_job_name):
        return self.parent_connector.env_manager.get_mainframe_meta_db_connector().execute_statement(f"CALL ADMIN_DS_BROWSE(3,'EDBPLA.OPCE.JCLLIB','{opc_job_name}','N',null,null)")


class Deployment:
    def __init__(self, connector):
        self.parent_connector=connector
        self.infa_connector = self.parent_connector.env_manager.get_connector(ResourceType.INFA, Env.DEV)
        self.hg_connector = self.parent_connector.env_manager.get_connector(ResourceType.HG)
        self.infa_connector =  self.parent_connector.env_manager.get_connector(ResourceType.INFA, Env.DEV)

    def get_last_created_label(self, label_prefix):
        return self.infa_connector.get_last_created_label(label_prefix)

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

    @RetryDecorator()
    def hg_deploy_file_change_commit(self, repository, label, tag):


        self.hg_connector.set_sub_folder(repository)

        self.hg_connector.pull()

        modify_cloning=input_YN("Do you want to change clonevar/clonelist variables?")
        use_clonevar=use_clonelist=None
        if modify_cloning:
            use_clonevar=input_YN("Do you want to use clonevar?")
            use_clonelist=input_YN("Do you want to use clonelist?")

        deploy_file_path=os.path.join(self.hg_connector.get_working_dir(),"PWC_deploy.txt")

        self.hg_deploy_file_set_label(deploy_file_path, label,
                                      modify_cloning=modify_cloning, clonevar=use_clonevar, clonelist=use_clonelist)
        print(f"{deploy_file_path} modified.")

        self.hg_connector.diff("PWC_deploy.txt")

        if not input_YN(f"Do you want to commit changes to PWC_deploy.txt (you can go now and edit {deploy_file_path} file manually)"):
               return False


        self.hg_connector.pull()
        self.hg_connector.add("PWC_deploy.txt")

        self.hg_connector.commit(label, "PWC_deploy.txt")

        id_res= self.hg_connector.run("id","-i", "--debug")

        id_stdout=id_res.stdout.decode("latin-1")
        changeset_id=id_stdout.strip().rstrip("+")
        print("Last changeset"+changeset_id)
        self.hg_connector.add_tag_to_commit(repository, changeset_id, tag)

        self.hg_connector.diff(".hgtags")

        if not input_YN(f"Do you want to commit changes to .hgtags"):
            return False
        self.hg_connector.commit(f"Added tag {tag} for changeset ", ".hgtags")

        self.hg_connector.run("outgoing", "-v")
        if not input_YN(f"Do you want to push above changes?"):
            return False
        self.hg_connector.push()

    def deploy_infa_by_label(self, target_env, label, confirm_all_safe=False, confirm_do_not_modify_pwc_deploy=False):
        # confirm_all_safe - automatically answer Y to all operations which are "safe"

        target_env=Env[target_env]
        self.hgprod_connector = self.parent_connector.env_manager.get_connector(ResourceType.HGPROD, target_env)
        self.jenkins_connector = self.parent_connector.env_manager.get_connector(ResourceType.JENKINS,  target_env)

        assert self.hgprod_connector.environment, f"HGProd connector for env {target_env} has not environment configured."

        if not self.infa_connector.check_label_exists(label):
            print(f"Label {label} does not exist.")
            if  input_YN(f"Do you want to create label {label}?"):
                self.infa_connector.create_label(label)
        else:
            print(f"Label {label} exists.")
            if confirm_all_safe or input_YN(f"Do you want to reuse label {label} to {target_env}?") :
                pass
            else:
                print("Done.")
                return False
        # Create deployment query

        if not self.infa_connector.check_query_exists(label):
            print(f"Depoyment query {label} does not exist.")
            if  input_YN(f"Do you want to create deployment query {label}?") :
                self.infa_connector.create_deployment_query(label)
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
        #checkedout_obj_df = self.infa_connector.find_checkout_as_pandas_df()
        labeled_obj=self.infa_connector.execute_query_as_pandas_df(label)

        #checkedout_labeled_df=labeled_obj.merge(checkedout_obj_df, on=["FOLDER","OBJECT_NAME"])

        #if len(checkedout_labeled_df)>0:
        #    print("Some labeled objects are not checked in:")
        #    print(checkedout_labeled_df)
        #    if input_YN("Do you want to continue?"):
        #        pass
        #    else:
        #        return False
        #else:
        #    print("All objects are checked in.")


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
                             f"{self.hgprod_connector.environment} and hgprod repository {repo}?") :
            pass
        else:
            print("Done.")
            return False;


        self.hgprod_connector.bestil(repo, tag)

        if confirm_all_safe or input_YN(f"Do you want to start jenkins build for environment {self.jenkins_connector.environment} and area {jenkins_area}?"):
            pass
        else:
            print("Done.")
            return False;


        ret, a= self.jenkins_connector.deploy_project(jenkins_area)
        return ret


    def deploy_infa_by_label_for_multiple_envs(self, target_envs, label, confirm_all_safe=False, confirm_do_not_modify_pwc_deploy=False):

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


class BECConnector(Connector):

    def __init__(self, key, env_manager: EnvManager, hg_repository_path=None):
        super().__init__(key)
        self.hg_repos_path=hg_repository_path
        self.env_manager = env_manager
        self.recon = Recon(self)
        self.deployment = Deployment(self)
        self.monitoring = Monitoring(self)

    def validate_config(self):
        super().validate_config()
        self.env_manager.validate_config()


    # def get_ftst2_db_connector(self):
    #     return  connector.get("DB/NZFTST2").set_data_source("MMDB")

    def get_bank_koncern_map_pd(self):
        sql= sql_meta.sql_ftst2_bank_koncern()
        print("SQL:\n"+sql)
        return self.env_manager.get_connector(ResourceType.DB,Env.FTST2,"NZ").query_pandas(sql)

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
