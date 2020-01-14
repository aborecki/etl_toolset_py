import datetime

from pyetltools.core import context
from pyetltools.jenkins.config import JenkinsConfig
from pyetltools.jenkins.connection import JenkinsConnection
from pyetltools.jenkins.context import JenkinsContext


def get_url_run_like_opc(env):
    if env == 'FTST2':
        return "/view/PWC10/view/PWC10_FTST/view/PWC10_FTST2/job/PWC10_runlikeopc-FTST2-execute/"
    raise Exception("env parameter not known.")


def check_date_YYYYMMDD(date):
    datetime.datetime.strptime(date, '%Y%m%d')


def manual_confirm():
    answer = input(f"Are you sure Y/N:")
    if answer != 'Y':
        raise Exception("Action canceled.")


class BECJenkinsContext(JenkinsContext):
    def __init__(self, config: JenkinsConfig, connection: JenkinsConnection):
        super().__init__(config, connection)

    def bec_start_recon(self, date):
        check_date_YYYYMMDD(date)
        manual_confirm()
        return self.bec_build_runlikeopc('FTST2','FBIXA990', date)

    def bec_start_push_recon(self, date):
        check_date_YYYYMMDD(date)
        manual_confirm()
        return self.bec_build_runlikeopc('FTST2','FBIXA980', date)

    def bec_build_runlikeopc(self, env, opcjob, bankdag):
        return self.build(get_url_run_like_opc(env),
              params={"runopc_opcjob": opcjob, "runopc_bankdag": bankdag})


    @classmethod
    def create_from_config(cls, config: JenkinsConfig):
        return BECJenkinsContext(config, JenkinsConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(JenkinsConfig, BECJenkinsContext)
