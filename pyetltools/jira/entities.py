

class JiraIssue:
    def __init__(self, response):
        self.response=response

    def get_sub_issues(self):
        return list(map(lambda r: JiraIssue(r), self.response["fields"]["subtasks"]))

    def get_summary(self):
        return self.response["fields"]["summary"]

    def get_id(self):
        return self.response["id"]

    def __repr__(self):
        return self.get_id()+":"+self.get_summary()