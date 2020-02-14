import copy
import json
import time

import requests

from pyetltools.core import connector
from pyetltools.core.connector import Connector


class JenkinsConnector(Connector):

    def __init__(self, key, url, username, password=None):
        super().__init__(key=key)
        self.url = url
        self.username = username
        self.password = password

    def get_url(self, suffix=""):
        return self.url.strip("/")+"/"+suffix.strip("/")

    def build(self, url, params=None):
        p=""
        if params is not None:
           p = "?"+"&".join([f"{key}={value}&" for (key, value) in params.items()])
        return self.request_post(url.rstrip("/")+"/buildWithParameters"+p)

    def wait_for_build_completion(self, response):
        location = response.headers["Location"]
        url = location + "api/json"
        buildUrl = None
        while not buildUrl:
            queue_status = json.loads(self.request_get(url).content)
            print(".", end="")
            if "url" in queue_status["executable"]:
                buildUrl = queue_status["executable"]["url"] + "api/json"
            time.sleep(1)

        print("Build URL:" + buildUrl)
        result = None
        while not result:
            build_status = json.loads(self.request_get(buildUrl).content)
            result = build_status["result"]
            print(".", end="")
            time.sleep(1)
        print(" BUILD RESULT:" + result)
        return result == "SUCCESS", result

    def request_post(self, url, data=None):
        return requests.post(url, auth=self.get_auth())

    def request_get(self, url, data=None):
        return requests.get(url, auth=self.get_auth())

    def get_auth(self):
        return self.username, self.get_password()

