from pyetltools.core import connector
from pyetltools.core.connector import Connector



class InfaCmdConnector(Connector):

    def __init__(self, key, template=None,
                 infacmd_cmd=None,
                 pmrep_cmd=None,
                 pmcmd_cmd=None,
                 username = None,
                 password=None,
                 repository = None,
                 domain = None,
                 security_domain = None
    ):
        super().__init__(key, password)
        self.infacmd_cmd=infacmd_cmd
        self.pmrep_cmd = pmrep_cmd
        self.pmcmd_cmd = pmcmd_cmd
        self.username = username
        self.repository = repository
        self.domain = domain
        self.security_domain = security_domain

    def run_pmrep_connect(self):
        return self.run_pmrep("connect", "-r", self.config.repository, "-d", self.config.domain,
                              "-s", self.config.security_domain, "-n", self.config.username, "-x", self.get_password())

    def run_pmrep_export_workflow(self, workflow, folder, output_file):
        return self.run_pmrep("objectexport", "-n", workflow, "-o", "workflow", "-f", folder, "-m", "-s", "-b", "-r",
                              "-u", output_file)

    # def run_pmrep_export_query(self, query, output_file):
    #     return self.run_pmrep("objectexport", "-n", query, "-o", "query",
    #                           "-u", output_file)

    def run_infacmd(self, *params):
        return self.config.infacmd_cmd.run(*params)

    def run_pmcmd(self, *params):
        return self.config.pmcmd_cmd.run(*params)

    def run_pmrep(self, *params):
        return self.config.pmrep_cmd.run(*params)

    def run_pmrep_create_label(self, label, comment):
        if comment:
            return self.run_pmrep("createlabel", "-a", label, "-c", comment)
        else:
            return self.run_pmrep("createlabel", "-a", label)

    def run_pmrep_create_query(self, query_name, query_type, expression):
        return self.run_pmrep("createquery", "-n", query_name, "-t", query_type, "-e", expression)

    def convert_log_bin_to_xml(self, xml_file_path):
        res = self.run_infacmd(["ConvertLogFile", "-fm", "XML", "-in", f"{xml_file_path}"])

