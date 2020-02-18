import copy
import datetime
import json

from pyetltools.core import connector
from pyetltools.jenkins.jenkins_connector import JenkinsConnector
import time


def get_url_suffix_run_like_opc(env):
    if env == 'FTST2':
        return "/view/PWC10/view/PWC10_FTST/view/PWC10_FTST2/job/PWC10_runlikeopc-FTST2-execute/"

    raise Exception("env parameter not known.")


def get_url_suffix_deploy(env, area):
    correct_envs=["TEST","FTST2"]
    correct_areas=["MDW", "RAP"]
    assert env in correct_envs, f"Environment {env} not supported by Jenkins connector. Supported envs: "+str(correct_envs)
    assert area in correct_areas, f"Area {area} not supported by Jenkins connector. Supported areas " + str(correct_areas)
    if env == "TEST":
        return f"view/PWC10/view/PWC10_{env}/job/PWC10_deployproject_{area}-{env}-Deploy/"
    if env.startswith("FTST"):
        return f"view/PWC10/view/PWC10_FTST/view/PWC10_{env}/job/PWC10_deployproject_{area}-{env}-Deploy/"

def check_date_YYYYMMDD(date):
    datetime.datetime.strptime(date, '%Y%m%d')


def manual_confirm():
    answer = input(f"Are you sure Y/N:")
    if answer != 'Y':
        raise Exception("Action canceled.")


class BECJenkinsConnector(JenkinsConnector):

    def __init__(self, key, url, username, password=None ,environment=None):
        super().__init__(key=key, url=url, username=username, password=password)
        self.environment=environment

    def clone(self):
        return copy.copy(self)

    def set_environment(self, environment):
        self.environment=environment
        return self

    def validate_config(self):
        super().validate_config()

    def bestil(self, area, wait_for_completition=True):
        return self.bestil_with_env(self.environment, area, wait_for_completition)

    def bestil_with_env(self, env, area, wait_for_completition=True):
        return self.build(get_url_suffix_deploy(env, area), wait_for_completition=wait_for_completition)

    def bec_start_recon(self, date, wait_for_completition=True):
        check_date_YYYYMMDD(date)
        return self.bec_build_runlikeopc('FTST2', 'FBIXA990', date, wait_for_completition=wait_for_completition)

    def bec_start_push_recon(self, date, wait_for_completition=True):
        check_date_YYYYMMDD(date)
        return self.bec_build_runlikeopc('FTST2', 'FBIXA980', date, wait_for_completition=wait_for_completition)

    def bec_build_runlikeopc(self, opcjob, bankdag=None, wait_for_completition=True):
        return self.bec_build_runlikeopc(self, self.environement, opcjob, bankdag=None, wait_for_completition=wait_for_completition)

    def bec_build_runlikeopc(self, env, opcjob, bankdag=None, wait_for_completition=True):

        params = {"runopc_opcjob": opcjob}
        if bankdag is not None:
            params["runopc_bankdag"] = bankdag
        url_suffix = get_url_suffix_run_like_opc(env)
        print("Running: " + url_suffix + " " + str(params))
        manual_confirm()
        return self.build(url_suffix,
                          params=params, wait_for_completition=wait_for_completition)





