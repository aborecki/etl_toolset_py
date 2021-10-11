import copy
import json
import time

import requests


from pyetltools.core.connector import Connector


class JenkinsConnector(Connector):

    def __init__(self, key, url, username, password=None):
        super().__init__(key=key, password=password)
        self.url = url
        self.username = username
        requests.packages.urllib3.disable_warnings()

    def validate_config(self):
        super().validate_config()

    def get_url(self, suffix=""):
        print(self.url)
        print(suffix)
        return self.url.strip("/")+"/"+suffix.strip("/")

    def build(self, url_suffix, params=None, wait_for_completition=True, wait_for_build_url=False):
        p=""
        if params is not None:
           p = "?"+"&".join([f"{key}={value}&" for (key, value) in params.items()])
        url = self.get_url(suffix=url_suffix)
        if params:
            url=url.rstrip("/")+"/buildWithParameters"+p
        else:
            url = url.rstrip("/") + "/build"
        print("Running build: "+url)
        response= self.request_post(url)
        if wait_for_build_url:
            return self.wait_for_completion(response, wait_for_build_url_only=True)
        if wait_for_completition:
            return self.wait_for_completion(response)
        return response

    def wait_for_completion(self, response, wait_for_build_url_only=False):
        location = response.headers["Location"]
        url = location + "api/json"
        buildUrl = None
        while not buildUrl:
            queue_status = json.loads(self.request_get(url).content)
            print(".", end="")
            if "executable" in queue_status and "url" in queue_status["executable"]:
                buildUrl = queue_status["executable"]["url"] + "api/json"
            time.sleep(5)
        print("")
        print("Build URL:" + buildUrl)
        if wait_for_build_url_only:
            return buildUrl
        result = None
        while not result:
            build_status = json.loads(self.request_get(buildUrl).content)
            result = build_status["result"]
            print(".", end="")
            time.sleep(5)
        print("")
        print(" BUILD RESULT:" + result)
        return result == "SUCCESS", result

    def check_build_status(self, buildUrl):
        build_status = json.loads(self.request_get(buildUrl).content)
        result = "N/A" if build_status["result"] is None else build_status["result"]
        print(" BUILD RESULT:" + result)
        return result

    def request_post(self, url, data=None):
        return requests.post(url, auth=self.get_auth(),  verify=False)

    def request_get(self, url, data=None):
        return requests.get(url, auth=self.get_auth(), verify=False)

    def get_auth(self):
        return self.username, self.get_password()

