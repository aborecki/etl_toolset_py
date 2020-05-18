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

    def validate_config(self):
        super().validate_config()


    def run_pmrep_connect(self):
        return self.run_pmrep("connect", "-r", self.repository, "-d", self.domain,
                              "-s", self.security_domain, "-n", self.username, "-x", self.get_password())

    def run_pmrep_export_workflow(self, workflow, folder, output_file):
        self.run_pmrep_connect()
        return self.run_pmrep("objectexport", "-n", workflow, "-o", "workflow", "-f", folder, "-m", "-s", "-b", "-r",
                              "-u", output_file)

    # def run_pmrep_export_query(self, query, output_file):
    #     return self.run_pmrep("objectexport", "-n", query, "-o", "query",
    #                           "-u", output_file)

    def run_infacmd(self, *params):
        return self.infacmd_cmd.run(*params)

    def run_pmcmd(self, *params):
        return self.pmcmd_cmd.run(*params)

    def run_pmrep(self, *params):
        return self.pmrep_cmd.run(*params)

    def run_pmrep_create_label(self, label, comment):
        self.run_pmrep_connect()
        if comment:
            return self.run_pmrep("createlabel", "-a", label, "-c", comment)
        else:
            return self.run_pmrep("createlabel", "-a", label)

    def run_pmrep_create_query(self, query_name, query_type, expression):
        self.run_pmrep_connect()
        return self.run_pmrep("createquery", "-n", query_name, "-t", query_type, "-e", expression)

    def run_pmrep_execute_query(self, query_name,sep=" "):
        self.run_pmrep_connect()
        return self.run_pmrep("executequery", "-q", query_name,"-b","-c",sep)


    def run_pmrep_find_checkout(self, sep=" "):
        #-u all users
        #-b verbose
        self.run_pmrep_connect()
        return self.run_pmrep("findcheckout","-u","-b","-c",sep)


    def run_pmrep_delete_query(self, query_name, query_type):
        self.run_pmrep_connect()
        return self.run_pmrep("deletequery", "-n", query_name,"-t",query_type,"-f")

    def convert_log_bin_to_xml(self, xml_file_path):
        self.run_pmrep_connect()
        res = self.run_infacmd("ConvertLogFile", "-fm", "XML", "-in", f"{xml_file_path}")

