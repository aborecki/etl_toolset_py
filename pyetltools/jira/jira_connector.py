import copy
import json
import logging

import requests

from pyetltools.core import connector
from pyetltools.core.connector import Connector

from pyetltools.jira.entities import JiraIssue
from pyetltools.jira import  request_templates


def print_response(response):
    print(json.dumps(response, indent=4, sort_keys=True))



class JiraConnector(Connector):

    def __init__(self, key, url, username, password=None):
        super().__init__(key=key)
        self.url = url
        self.username = username
        self.password = password

    def validate_config(self):
        super().validate_config()

    def get_url(self, suffix):
        return self.url.strip("/")+"/"+suffix.strip("/")

    def get_headers(self):
        return request_templates.get_authentication_headers(self.username, self.get_password())

    def request_get(self,url_suffix):
        response = requests.get(self.get_url(url_suffix), headers=self.get_headers())
        try:
            response = json.loads(response.content)
        except Exception as e:
            print("Cannot parse json. " + str(response))
            raise e
        return response

    def request_post(self,url_suffix, data):
        response = requests.post(self.get_url(url_suffix), data=data, headers=self.get_headers())
        return

    def request_put(self,url_suffix, data):
        response = requests.put(self.get_url(url_suffix), data=data, headers=self.get_headers())
        return response

    def get_issue(self, issue_id) -> JiraIssue:
        logging.debug("get_issue:"+issue_id)
        return JiraIssue.from_content(self.request_get(f"/issue/{issue_id}"))


    def search_issues(self, jql_query):
        logging.debug("search_issues:" + jql_query)
        return JiraIssue.from_search_result(self.request_get(f"/search?jql={jql_query}"))

    def create_subissue(self, project_key, issue_id, subissue_summary, subissue_description):
        data=request_templates.get_create_subissue_body_json(project_key, issue_id, subissue_summary, subissue_description)
        return self.request_post(f"issue",data=data)

    def update_issue(self, jira: JiraIssue):
        upd_content=copy.deepcopy(jira.content)
        upd_content["fields"].pop("status") # status cannot be updated
        return self.request_put(f"issue/{jira.get_key()}", data=json.dumps(upd_content))
