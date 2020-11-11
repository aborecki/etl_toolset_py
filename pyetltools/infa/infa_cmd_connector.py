from pyetltools.core import connector
from pyetltools.core.connector import Connector
import os
from pyetltools.infa import lib
from pyetltools.infa.lib import load_infa_log_to_df


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

    def run_pmrep_export_object(self, obj_name, obj_type, folder, output_file):
        self.run_pmrep_connect()
        return self.run_pmrep("objectexport", "-n", obj_name, "-o", obj_type, "-f", folder, "-m", "-s", "-b", "-r",
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


    def run_pmcmd_start_workflow(self, integration_service, folder_name, workflow_name, run_instance=None):
        if run_instance:
            return self.run_pmcmd("startworkflow", "-sv", integration_service, "-f", folder_name, "-d", self.domain,
                                  "-usd", self.security_domain, "-u", self.username, "-p", self.get_password(), "-rin",run_instance,
                                  workflow_name)
        else:
            return self.run_pmcmd("startworkflow", "-sv", integration_service, "-f", folder_name,  "-d", self.domain,
                              "-usd", self.security_domain, "-u", self.username, "-p", self.get_password(), workflow_name)


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

    def run_infacmd_convert_log_bin_to_xml(self, bin_file_input_path, xml_output=None):
        if not  xml_output:
            res = self.run_infacmd("ConvertLogFile", "-fm", "XML", "-in", f"{bin_file_input_path}")
        else:
            res = self.run_infacmd("ConvertLogFile", "-fm", "XML", "-in", f"{bin_file_input_path}","-lo", f"{xml_output}")
        return res


    def check_if_file_exists_and_contains(self,file, str):
        if not os.path.isfile(file):
            return False
        else:
            with open(file, 'rt') as file:
                data = file.read().replace('\n', '')
        if str in data:
            return True
        else:
            return False


    def parse_log(self,output_file, log_df, output_only_matched=True):
        parsed = lib.parse_infa_log(output_file, log_df, output_only_matched=output_only_matched)
        return parsed

    def load_log(self, output_file):
        return load_infa_log_to_df(output_file)

    def convert_log_bin_to_xml(self, bin_file_input_path, output_file , use_converted_files=False, overwrite_converted_files=False ):
        res=None
        if self.check_if_file_exists_and_contains(output_file,"completed at") and use_converted_files:
            pass
        else:
            if self.check_if_file_exists_and_contains(output_file,"completed at") and not overwrite_converted_files:
                raise Exception("File "+ output_file + " already exists. Set parameter use_converted_files or overwrite_converted_files to True." )
            else:
                res = self.run_infacmd_convert_log_bin_to_xml(bin_file_input_path, output_file)
        return res

