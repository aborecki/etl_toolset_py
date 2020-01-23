import os
import subprocess
import pyetltools


def convert_infa_bin_log_to_txt(path):
    infacmd_path = pyetltools.context.get_config("INFACMD_DIR").value
    infahome = pyetltools.context.get_config("INFA9_HOME_DIR").value
    os.environ['INFA_HOME'] = infahome
    os.environ['INFA_DOMAINS_FILE'] = infahome + r"\clients\PowerCenterClient\domains.infa"

    print(" ".join([infacmd_path + "\\infacmd.bat", "ConvertLogFile", "-fm Text", f"-in {path}"]))
    res = subprocess.run([infacmd_path + "/infacmd.bat", "ConvertLogFile", "-fm", "XML", "-in", f"{path}"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(res.stdout)
    print(res.stderr)
