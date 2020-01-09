import json
import logging

from pyetltools.core import context
from pyetltools.core.context import Context
from pyetltools.jira.config import JiraConfig
from pyetltools.jira.connection import JiraConnection
from pyetltools.jira.entities import JiraIssue
from pyetltools.jira.request_templates import get_create_subtask_body


def print_response(response):
    print(json.dumps(response, indent=4, sort_keys=True))


def get_subtasks_from_issue(issue_response):
    return issue_response["fields"]["subtasks"]


class JiraContext(Context):

    def __init__(self, config: JiraConfig, connection: JiraConnection):
        self.config = config
        self.connection = connection

    def  get_issue(self, issue_id) -> JiraIssue:
        logging.debug("get_issue:"+issue_id)
        return JiraIssue(self.connection.execute_request(f"issue/{issue_id}"))

    def create_subtask(self, project_key, issue_id, subtask_summary, subtask_description):
        data=get_create_subtask_body(project_key, issue_id, subtask_summary, subtask_description)
        return JiraIssue(self.connection.execute_post(f"issue",data))

    @classmethod
    def create_from_config(cls, config: JiraConfig):
        return JiraContext(config,  JiraConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(JiraConfig, JiraContext)