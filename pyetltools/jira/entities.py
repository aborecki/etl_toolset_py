import copy
import json


class JiraIssue:
    def __init__(self, content):
        self.content=content
        self.content_original=copy.deepcopy(content)

    def get_sub_issues(self):
        return list(map(lambda r: JiraIssue(r), self.content["fields"]["subtasks"]))

    def set_summary(self,new_summary):
        self.content["fields"]["summary"]=new_summary

    def get_summary(self):
        return self.content["fields"]["summary"]

    def set_description(self, new_desc):
        self.content["fields"]["description"]=new_desc

    def get_description(self):
        return self.content["fields"]["description"]

    def get_sprint(self):
        return self.content["fields"]["customfield_10004"]

    def set_sprint(self, new_sprint):
        self.content["fields"]["customfield_10004"] = new_sprint

    def get_id(self):
        return self.content["id"]

    def get_key(self):
        return self.content["key"]

    def set_assignee_name(self, key):
        if "assignee" not in  self.content["fields"]:
            self.content["fields"]["assignee"]={}
        if not self.content["fields"]["assignee"]:
            self.content["fields"]["assignee"]={}
        self.content["fields"]["assignee"]["name"] = key

    def get_assignee_name(self):
        return self.content["fields"]["assignee"]["name"]

    def __repr__(self):
        return self.get_id()+":"+self.get_summary()


    @classmethod
    def from_content(cls, content):
        try:
            return JiraIssue(content)
        except:
            return content

    @classmethod
    def from_search_result(cls, content):
        if "total" not in content:
            print(content)
            return
        print("total:"+str(content["total"]))
        print("startAt:" + str(content["startAt"]))
        print("maxResults:" + str(content["maxResults"]))
        return [JiraIssue.from_content(i) for i in content["issues"]]
