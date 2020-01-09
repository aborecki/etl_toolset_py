from pyetltools.core import context
from pyetltools.jenkins.config import JenkinsConfig
from pyetltools.jenkins.connection import JenkinsConnection
from pyetltools.jenkins.context import JenkinsContext


def get_url_run_like_opc():
    return "/view/PWC10/view/PWC10_FTST/view/PWC10_FTST2/job/PWC10_runlikeopc-FTST2-execute/"


class BECJenkinsContext(JenkinsContext):
    def __init__(self, config: JenkinsConfig, connection: JenkinsConnection):
        super().__init__(config, connection)

    @classmethod
    def create_from_config(cls, config: JenkinsConfig):
        return BECJenkinsContext(config, JenkinsConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(JenkinsConfig, BECJenkinsContext)
