import copy
import re
import time

from mechanize import Browser

from pyetltools.core import connector
from pyetltools.core.connector import Connector
import mechanize
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class HGProdConnector(Connector):
    def __init__(self, key, url, username, password=None, environment=None):
        super().__init__(key=key, password=password)
        self.url=url
        self.username=username
        self.environment=environment

    def validate_config(self):
        super().validate_config()


    def clone(self):
        return copy.copy(self)

    def set_environment(self, env):
        self.environment=env
        return self

    LOGIN_PAGE = "accounts/login/?next=/"
    REPOS_PAGE = "bestsys/sel_repo"

    def get_url(self, suffix):
        return urljoin(self.url, suffix)

    def get_case_insensitive_key_value(self, input_dict, key):
        return next((value for dict_key, value in input_dict.items() if dict_key.lower() == key.lower()), None)

    def login(self):
        browser = mechanize.Browser()
        browser.open(self.get_url(self.LOGIN_PAGE))
        browser.select_form(action="/accounts/login/")
        browser.form["username"] = self.username
        browser.form["password"] = self.get_password()
        browser.submit()

        return browser

    def get_repos(self, browser):
        page = browser.open(self.get_url(self.REPOS_PAGE))
        return self.extract_links_from_table(page.read(), ["bestil","status"], col_nr=0, table_class="datatable")

    def extract_links_from_table(self, html, link_texts=[], col_nr=0, table_class=None):
        soup = BeautifulSoup(html, features="html5lib")
        table = soup.findAll("table", class_=table_class)
        res = {}
        for row in table[0].findAll('tr')[1:]:
            col = row.findAll('td')
            repo = col[col_nr].string.strip()
            links={}
            for l in link_texts:
                href = self.find_link_with_text(row, l)["href"]
                links[l]=href
            res[repo] = links
        return res

    def extract_columns_from_table(self, html, col_nrs=[0], table_class=None):
        soup = BeautifulSoup(html, features="html5lib")
        table = soup.findAll("table", class_=table_class)
        res = []
        for row in table[0].findAll('tr')[1:]:
            col = row.findAll('td')
            row=[]
            for i in col_nrs:
                row.append(col[i].text.strip())
            res.append(row)
        return res

    def extract_links_from_list(self, html, element_class_=None):
        soup = BeautifulSoup(html, features="html5lib")
        content = soup.find(id="content")
        list = content.findAll("dl")
        res = {}
        for elem in list[0].findAll('dd', class_=element_class_):
            env = elem.text.strip()
            href = elem.find("a")["href"]
            res[env] = href
        return res

    def find_link_with_text(self, tag, text):
        for a in tag.findAll("a"):
            if text.lower() in a.string.strip().lower():
                return a

    def get_tag_for_env(self, browser, status_url, env):
        status_page = browser.open(self.get_url(status_url))
        status_table = self.extract_columns_from_table(status_page.read(), col_nrs=[0, 1], table_class="datatable")
        status_env_tag_dict = dict(status_table)
        return status_env_tag_dict[env]


    def bestil(self, repository, tag, wait_for_completion=True):
        return self.bestil_with_env(repository, tag, self.environment, wait_for_completion)

    def bestil_with_env(self, repo, tag, env, wait_for_completion=True):

        print("Logging in", end=" ")
        browser = self.login()
        print("DONE")
        print("Getting repository list", end=" ")
        repos_list = self.get_repos(browser)
        print("DONE")
        tags_url = self.get_case_insensitive_key_value(repos_list, repo)["bestil"]
        status_page=self.get_case_insensitive_key_value(repos_list, repo)["status"]
        if repo not in repos_list:
            print(sorted(repos_list.keys(),key=str.casefold))
            print(f"{repo} not in list of repos.")
            return False
        print(f"Getting tags list for repository {repo}", end=" ")
        tags_page = browser.open(self.get_url(tags_url))
        print("DONE")
        tags_page_content=tags_page.read()
        tags_list = self.extract_links_from_table(tags_page_content, ["bestil"], col_nr=0, table_class="datatable")

        tag_urls=self.get_case_insensitive_key_value(tags_list, tag)


        if not tag_urls:
            print(sorted(tags_list.keys(),key=str.casefold))
            print(f"{tag} not in list of tags for repo {repo}.")
            return False
        envs_url = tag_urls["bestil"]
        print(f"Getting environements list for repository {repo} and tag {tag}", end=" ")
        envs_page = browser.open(self.get_url(envs_url))
        print("DONE")
        envs_list = self.extract_links_from_list(envs_page.read(), element_class_="Contents")

        bestil_url = self.get_case_insensitive_key_value(envs_list, env)
        if not bestil_url:
            print(sorted(envs_list.keys(),key=str.casefold))
            print(f"{env} environment is not in list of envs for tag {tag} and repo {repo}.")
            return False

        print("Opening bestil submit page", end=" ")
        bestil_page=browser.open(bestil_url)
        print("DONE")
        assert len(browser.forms())==1, "More forms then one"
        print("Submitting", end=" ")

        browser.select_form(nr=0)
        browser.submit()
        print("DONE")
        if wait_for_completion:
            print("Waiting for bestil completition.")
            while True:
                time.sleep(5)
                current_tag=self.get_tag_for_env(browser, status_page, env)
                if current_tag.upper()==tag.upper():
                    print(f"Tag for {env} is {current_tag}.")
                    print("Bestil complete.")
                    break;
                else:
                    print(f"Tag for {env} is {current_tag}. Waiting for {tag} ")
        return True

