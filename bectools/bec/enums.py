from enum import Enum

class ResourceType(Enum):
    DB="DB"
    HGPROD="HGPROD"
    JENKINS= "JENKINS"
    JIRA="JIRA"
    INFA="INFA"

class Env(Enum):
    DEV="DEV"
    TEST="TEST"
    UTST="UTST"
    FTST2="FTST2"
    FOPROD="FOPROD"
    PROD="PROD"