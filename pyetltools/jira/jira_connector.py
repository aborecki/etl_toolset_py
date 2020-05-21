import copy
import json
import logging
import urllib

import requests

from pyetltools.core import connector
from pyetltools.core.connector import Connector

from pyetltools.jira.entities import JiraIssue
from pyetltools.jira import  request_templates
from xml.sax.saxutils import escape
def diff(old, new):
    if  type(old) != type(new):
        return (True, new)
    if isinstance(old, dict):
        ret={}
        for k,v in new.items():
            if k not in old:
                ret[k]=v
            else:
                old_val=old[k]
                is_diff, res=diff(old_val, v)
                if is_diff:
                     ret[k]=res
        if len(ret) >0:
            return (True, ret)
        else:
            return (False, None)
    else:
        return (True,new) if old!=new else (False,None)

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
        print(url_suffix)
        response = requests.get(self.get_url(url_suffix), headers=self.get_headers(), verify=False)
        try:
            response = json.loads(response.content)
        except Exception as e:
            print("Cannot parse json. " + str(response))
            raise e
        return response

    def request_post(self,url_suffix, data):
        response = requests.post(self.get_url(url_suffix), data=data, headers=self.get_headers())
        return response

    def request_put(self,url_suffix, data):
        response = requests.put(self.get_url(url_suffix), data=data, headers=self.get_headers(), verify=False)
        return response

    def get_issue(self, issue_id) -> JiraIssue:
        logging.debug("get_issue:"+issue_id)
        return JiraIssue.from_content(self.request_get(f"/issue/{issue_id}"))


    def search_issues(self, jql_query, startAt=None, maxResults=None):
        logging.debug("search_issues:" + jql_query)
        query_params={"jql":jql_query}

        if startAt:
            query_params["startAt"]=str(startAt)
        if maxResults:
            query_params["maxResults"]=str(maxResults)
        query_params=urllib.parse.urlencode(query_params)
        return JiraIssue.from_search_result(self.request_get(f"/search?{query_params}"))

    def create_subissue(self, project_key, issue_id, subissue_summary, subissue_description):
        data=request_templates.get_create_subissue_body_json(project_key, issue_id,
                                                             subissue_summary,
                                                             subissue_description)
        return self.request_post(f"issue",data=data)

    def move_subissue(self, issue_id_or_key, from_,to):
        content=json.dumps({"original":from_, "current":to})
        return self.request_post(f"issue/{issue_id_or_key}/subtask/move", data=content)


    def update_issue(self, jira: JiraIssue):
        #upd_content=copy.deepcopy(jira.content)
        #upd_content["fields"].pop("status") # status cannot be updated

        changed, updated_fields=diff(jira.content_original, jira.content)
        if not changed:
            print("Nothing to update")
            return
        print(json.dumps(updated_fields))
        #updated_fields["key"]=jira.content_original["key"]
        #updated_fields["id"] = jira.content_original["id"]
        if "fields" not in updated_fields:
            updated_fields["fields"]={}
        #updated_fields["fields"]["issuetype"] = jira.content_original["fields"]["issuetype"]
        #print(json.dumps(updated_fields))
        return self.request_put(f"issue/{jira.get_key()}?overrideScreenSecurity=true", data=json.dumps(updated_fields))
