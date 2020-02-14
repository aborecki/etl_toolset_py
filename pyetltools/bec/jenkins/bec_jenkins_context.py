import datetime
import json

from pyetltools.core import connector
from pyetltools.jenkins.jenkins_connector import JenkinsConnector
import time


def get_url_suffix_run_like_opc(env):
    if env == 'FTST2':
        return "/view/PWC10/view/PWC10_FTST/view/PWC10_FTST2/job/PWC10_runlikeopc-FTST2-execute/"
    raise Exception("env parameter not known.")


def check_date_YYYYMMDD(date):
    datetime.datetime.strptime(date, '%Y%m%d')


def manual_confirm():
    answer = input(f"Are you sure Y/N:")
    if answer != 'Y':
        raise Exception("Action canceled.")


class BECJenkinsConnector(JenkinsConnector):

    def bec_start_recon(self, date):
        check_date_YYYYMMDD(date)
        return self.bec_build_runlikeopc('FTST2', 'FBIXA990', date)

    def bec_start_push_recon(self, date):
        check_date_YYYYMMDD(date)
        return self.bec_build_runlikeopc('FTST2', 'FBIXA980', date)

    def bec_build_runlikeopc(self, env, opcjob, bankdag=None):
        url = self.get_url(suffix=get_url_suffix_run_like_opc(env))
        params = {"runopc_opcjob": opcjob}
        if bankdag is not None:
            params["runopc_bankdag"] = bankdag
        print("Running: " + url + " " + str(params))
        manual_confirm()
        return self.build(url,
                          params=params)

    def bec_build_runlikeopc_wait_complete(self, env, opcjob, bankdag=None):
        response = self.bec_build_runlikeopc(env, opcjob, bankdag)
        return self.wait_for_build_completion(response)


