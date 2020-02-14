import json


class JiraIssue:
    def __init__(self, content):
        self.content=content

    def get_sub_issues(self):
        return list(map(lambda r: JiraIssue(r), self.content["fields"]["subtasks"]))

    def set_summary(self,new_summary):
        self.content["fields"]["summary"]=new_summary

    def get_summary(self):
        return self.content["fields"]["summary"]

    def get_id(self):
        return self.content["id"]

    def get_key(self):
        return self.content["key"]

    def __repr__(self):
        return self.get_id()+":"+self.get_summary()


    @classmethod
    def from_content(cls, content):
        return JiraIssue(json)

    def from_search_result(self, content):
        return []
