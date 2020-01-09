import logging, sys

import pyetltools
from pyetltools.data.core.context import DBContext
from pyetltools.jenkins.context import JenkinsContext
from pyetltools.jira import context
from pyetltools.jira.context import JiraContext

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)


logging.info(__name__+":__init__.py")


DBContext.register_context_factory()
JenkinsContext.register_context_factory()
JiraContext.register_context_factory()

context = pyetltools.core.context

jira_helper=context



