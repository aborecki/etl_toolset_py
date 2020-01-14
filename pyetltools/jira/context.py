import copy
import json
import logging

from pyetltools.core import context
from pyetltools.core.context import Context
from pyetltools.jira.config import JiraConfig
from pyetltools.jira.connection import JiraConnection
from pyetltools.jira.entities import JiraIssue
from pyetltools.jira.request_templates import  get_create_subissue_body


def print_response(response):
    print(json.dumps(response, indent=4, sort_keys=True))



class JiraContext(Context):

    def __init__(self, config: JiraConfig, connection: JiraConnection):
        self.config = config
        self.connection = connection

    def get_issue(self, issue_id) -> JiraIssue:
        logging.debug("get_issue:"+issue_id)
        response=self.connection.execute_request(f"issue/{issue_id}")
        try:
            issue_content = json.loads(response.content)
        except Exception as e:
            print("get_issue: Cannot parse json. "+response)
            raise e

        return JiraIssue(issue_content)

    def create_subissue(self, project_key, issue_id, subissue_summary, subissue_description):
        data=get_create_subissue_body(project_key, issue_id, subissue_summary, subissue_description)
        return JiraIssue(self.connection.execute_post(f"issue",data))

    def update_issue(self, jira: JiraIssue):

        upd_content=copy.deepcopy(jira.content)
        upd_content["fields"].pop("status") # status cannot be updated
        return self.connection.execute_put(f"issue/"+jira.get_key(), data=json.dumps(upd_content))

    @classmethod
    def create_from_config(cls, config: JiraConfig):
        return JiraContext(config,  JiraConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(JiraConfig, JiraContext)