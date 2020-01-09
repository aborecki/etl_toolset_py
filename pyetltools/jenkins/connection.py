import logging

from pyetltools.core import  connection
from pyetltools.core.connection import Connection
from pyetltools.jenkins.config import JenkinsConfig
import requests
import json


class JenkinsConnection(Connection):

    def __init__(self, config: JenkinsConfig):
        super().__init__(config)


    def execute_post(self, request_url_suffix, data) :
        logging.debug("DATA:"+str(data))
        request_url_suffix = request_url_suffix.strip("/")
        res = requests.post(self.config.url.rstrip('/')+"/"+request_url_suffix, data=data,
                            auth=(self.config.username, self.get_password()))
        return res

    def execute_get(self, request_url_suffix):
        request_url_suffix=request_url_suffix.strip("/")
        res = requests.get(self.config.url.rstrip("/")+ "/" +request_url_suffix,
                           auth=(self.config.username, self.get_password()))
        return res


