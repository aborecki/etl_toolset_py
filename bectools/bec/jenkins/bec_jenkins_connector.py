import copy
import datetime
import json

from pyetltools.core import connector
from pyetltools.jenkins.jenkins_connector import JenkinsConnector
import time
import re

def get_url_suffix_run_like_opc(env):
    correct_envs=["DEV","TEST","FTST","UTST"]

    assert re.sub(r'\d+', '', env) in correct_envs, f"Environment {env} not supported by Jenkins connector. Supported envs: "+str(correct_envs)

    if env.startswith("FTST"):
        return f"/view/PWC10/view/PWC10_FTST/view/PWC10_FTST2/job/PWC10_runlikeopc-{env}-execute/"
    else:
        return f"view/PWC10/view/PWC10_{env}/job/PWC10_runlikeopc-{env}-execute/"


    raise Exception(f"env {env} parameter not known.")


def get_url_suffix_deploy(env, area):
    correct_envs=["DEV","TEST","FTST","UTST"]

    assert re.sub(r'\d+', '', env) in correct_envs, f"Environment {env} not supported by Jenkins connector. Supported envs: "+str(correct_envs)

    if env.startswith("FTST"):
        return f"view/PWC10/view/PWC10_FTST/view/PWC10_{env}/job/PWC10_deployproject_{area}-{env}-Deploy/"
    else:
        return f"view/PWC10/view/PWC10_{env}/job/PWC10_deployproject_{area}-{env}-Deploy/"

def check_date_YYYYMMDD(date):
    datetime.datetime.strptime(date, '%Y%m%d')



def manual_confirm():
    m=f"Are you sure Y/N:"
    answer = input(m)
    while answer.upper() not in ['Y','N']:
        print("Y(Yes)/N(No) ?")
        answer = input(m)
    if  answer.upper() =="N":
        raise Exception("Action canceled.")


class BECJenkinsConnector(JenkinsConnector):

    def __init__(self, key, url, username, password=None ,environment=None):
        super().__init__(key=key, url=url, username=username, password=password)
        self.environment=environment

    def clone(self):
        cp=BECJenkinsConnector(self.key, self.url, self.username, self.password, self.environment)
        return cp

    def with_environment(self, environment):
        cp=self.clone()
        cp.environment=environment
        return cp

    def validate_config(self):
        super().validate_config()

    def deploy_project(self, area, wait_for_completition=True):
        return self.deploy_project_with_env(self.environment, area, wait_for_completition)

    def deploy_project_with_env(self, env, area, wait_for_completition=True):
        return self.build(get_url_suffix_deploy(env, area), wait_for_completition=wait_for_completition)

    def bec_start_recon(self, date, wait_for_completition=True):
        check_date_YYYYMMDD(date)
        return self.bec_build_runlikeopc('FTST2', 'FBIXA990', date, wait_for_completition=wait_for_completition)

    def bec_start_push_recon(self, date, wait_for_completition=True):
        check_date_YYYYMMDD(date)
        return self.bec_build_runlikeopc('FTST2', 'FBIXA980', date, wait_for_completition=wait_for_completition)

    def bec_start_init(self, date, wait_for_completition=True):
        return self.bec_build_runlikeopc(self.environment, '.MDWINI0', date, wait_for_completition=wait_for_completition)

    def bec_start_wf_extract_NZ_information(self, date, wait_for_completition=True):
        return self.bec_build_runlikeopc(self.environment, '.EDWM050 ', date, wait_for_completition=wait_for_completition)

    def bec_start_wf_extract_NZCAT_tables(self, date, wait_for_completition=True):
        return self.bec_build_runlikeopc(self.environment, '.EDWM051', date, wait_for_completition=wait_for_completition)

    def bec_build_runlikeopc(self, opcjob, bankdag=None, wait_for_completition=True):
        return self.bec_build_runlikeopc(self, self.environement, opcjob, bankdag=None, wait_for_completition=wait_for_completition)

    def bec_build_runlikeopc(self, env, opcjob, bankdag=None, wait_for_completition=True, confirm=False):

        params = {"runopc_opcjob": opcjob}
        if bankdag is not None:
            params["runopc_bankdag"] = bankdag
        url_suffix = get_url_suffix_run_like_opc(env)
        print("Running: " + url_suffix + " " + str(params))
        if not confirm:
            manual_confirm()
        return self.build(url_suffix,
                          params=params, wait_for_completition=wait_for_completition)





