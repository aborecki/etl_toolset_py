from pyetltools.core import context
from pyetltools.core.context import Context
from pyetltools.jenkins.config import JenkinsConfig
from pyetltools.jenkins.connection import JenkinsConnection


class JenkinsContext(Context):

    def __init__(self, config: JenkinsConfig, connection: JenkinsConnection):
        self.config = config
        self.connection = connection

    def build(self, url, params=None):

        p = None;
        if params is not None:
            p="?"
            for (key, value) in params.items():
                p = f"{p}{key}={value}&"
            p = p.rstrip("&")

        return self.connection.execute_post(url.rstrip("/")+"/buildWithParameters"+p, data=None)


    @classmethod
    def create_from_config(cls, config: JenkinsConfig):
        return JenkinsContext(config, JenkinsConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(JenkinsConfig, JenkinsContext)
