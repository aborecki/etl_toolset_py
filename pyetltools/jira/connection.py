import logging


from pyetltools.core.connection import Connection
from pyetltools.jira.config import JiraConfig
import pyetltools.jira.request_templates as t

import requests



class JiraConnection(Connection):

    def __init__(self, config: JiraConfig):
        super().__init__(config)
        self._auth_headers = t.get_authentication_headers(self.config.username, self.get_password())

    def execute_request(self, request_url_suffix):
        res = requests.get(self.config.url.rstrip('/') + "/" + request_url_suffix, headers=self._auth_headers)
        return res

    def execute_post(self, request_url_suffix, data):
        logging.debug("DATA:" + data)
        res = requests.post(self.config.url.rstrip('/') + "/" + request_url_suffix, data=data,
                            headers=self._auth_headers)
        return res

    def execute_put(self, request_url_suffix, data):
        logging.debug("DATA:" + data)
        res = requests.put(self.config.url.rstrip('/') + "/" + request_url_suffix, data=data,
                           headers=self._auth_headers)
        return res
