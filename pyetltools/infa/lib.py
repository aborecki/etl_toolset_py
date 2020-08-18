import lxml.etree as ET
from collections import namedtuple
import re


def parse_infa_log(xml_file, output_only_matched=True):
    import pandas
    LogEntry = namedtuple("LogEntry",
                          "timestamp message messageCode service clientNode threadName lookup_rows_count lookup_trans lookup_trans_sql lookup_trans_sql_default_override  src_trg_type src_trg_table src_trg_instance src_trg_output_rows src_trg_affected_rows src_trg_applied_rows src_trg_rejected_rows sq_instance sq_sql sq_reader_src_table sq_reader_instance sq_reader_rows sq_reader_error_rows param_type param_value param_name log_workflow_name instance_name log_workflow_run_id")
    parsed_xml = ET.parse(xml_file)
    log_entries = parsed_xml.findall(".//logEvent")
    print("Log entries read:" + str(len(log_entries)))
    ret = []
    last_src_trg_type = None
    log_workflow_run_id = None
    bankdato_date_param = None
    for l in log_entries:
        from datetime import datetime
        timestamp_orig = int(str(l.attrib["timestamp"])) / 1000
        timestamp = datetime.fromtimestamp(timestamp_orig)
        messageCode = str(l.attrib["messageCode"])
        message = str(l.attrib["message"])
        service = str(l.attrib["service"])
        clientNode = str(l.attrib["clientNode"])
        threadName = str(l.attrib["threadName"])
        data = parse_log_message(message)
        if not data.matched and output_only_matched:
            continue


        lookup_rows_count = None

        lookup_trans=None
        lookup_trans_sql=None
        lookup_trans_sql_default_override=None

        src_trg_type = None
        src_trg_table = None
        src_trg_instance = None
        src_trg_output_rows = None
        src_trg_affected_rows = None
        src_trg_applied_rows = None
        src_trg_rejected_rows = None

        sq_instance = None
        sq_sql = None

        sq_reader_src_table = None
        sq_reader_instance = None
        sq_reader_rows = None
        sq_reader_error_rows = None

        param_value = None
        param_name = None
        param_type = None

        log_workflow_name = None
        instance_name = None


        if len(data.src_trg_type) > 0:
            last_src_trg_type = data.src_trg_type[0]

        if len(data.lookup) > 0:
            lookup_rows_count = data.lookup[0]
        if len(data.lookup_trans) > 0:
            lookup_trans = data.lookup_trans[0][0]
            override_text=data.lookup_trans[0][1].upper()
            if  "DEFAULT" in  override_text:
                lookup_trans_sql_default_override="default sql"
            elif "OVERRIDE" in  override_text:
                lookup_trans_sql_default_override="override sql"
            lookup_trans_sql = data.lookup_trans[0][2]
        if len(data.targets) > 0:
            src_trg_type = last_src_trg_type
            src_trg_table = data.targets[0][0]
            src_trg_instance = data.targets[0][1]
            src_trg_output_rows = data.targets[0][2]
            src_trg_affected_rows = data.targets[0][3]
            src_trg_applied_rows = data.targets[0][4]
            src_trg_rejected_rows = data.targets[0][5]
        if len(data.sq_instances) > 0:
            sq_instance = data.sq_instances[0][0]
            sq_sql = data.sq_instances[0][1]
        if len(data.sq_reader) > 0:
            sq_reader_src_table = data.sq_reader[0][2]
            sq_reader_instance = data.sq_reader[0][3]
            sq_reader_rows = data.sq_reader[0][0]
            sq_reader_error_rows = data.sq_reader[0][1]

        if len(data.params) > 0:
            param_type = "default mapping"
            param_value = data.params[0][0]
            param_name = data.params[0][1]

        if len(data.params_wf) > 0:
            param_type = "override workflow"
            param_value = data.params_wf[0][0]
            param_name = data.params_wf[0][1]

        if len(data.params) > 0 or len(data.params_wf) > 0:
            if "bankdato_date" in param_name.lower() and re.match(r"\d\d\d\d-\d\d-\d\d", param_value):
                if bankdato_date_param is not None and bankdato_date_param !=param_value:
                    raise Exception("Multiple values for bankdato_date in one log file "+xml_file)
                bankdato_date_param=param_value


        if len(data.params_pers) > 0:
            param_type = "persisted mapping"
            param_value = data.params_pers[0][0]
            param_name = data.params_pers[0][1]

        if len(data.wf_run_info) > 0:
            if log_workflow_run_id is not None:
                raise Exception("Multiple WF run information found in single log file "+ xml_file)
            log_workflow_name = data.wf_run_info[0][0]
            instance_name = data.wf_run_info[0][1]
            log_workflow_run_id = data.wf_run_info[0][2]

        ret.append(LogEntry(timestamp, message, messageCode, service, clientNode, threadName,
                            lookup_rows_count, lookup_trans, lookup_trans_sql,lookup_trans_sql_default_override, src_trg_type, src_trg_table, src_trg_instance, src_trg_output_rows,
                            src_trg_affected_rows,
                            src_trg_applied_rows, src_trg_rejected_rows, sq_instance, sq_sql,
                            sq_reader_src_table, sq_reader_instance, sq_reader_rows, sq_reader_error_rows,
                            param_type, param_value, param_name,
                            log_workflow_name, instance_name, log_workflow_run_id))
    ret=pandas.DataFrame(data=ret)

    ret["log_workflow_run_id"]=log_workflow_run_id
    ret["bankdato_date_param"] = bankdato_date_param
    return ret

src_tgt_summary_pat = re.compile('(.*) Load Summary.')
lookup_pat = re.compile('Lookup table row count : (\\d+)')
lookup_trans_pat = re.compile(r'Lookup Transformation \[([^]]+)\]: (Default sql to create lookup cache|Lookup override sql to create cache): (.*)', re.DOTALL)
target_pat = re.compile(
    r'Table: \[(\w+)\] \(Instance Name: \[([^]]+)\]\)\s+Output Rows \[(\d+)\], Affected Rows \[(\d+)\], Applied Rows \[(\d+)\], Rejected Rows \[(\d+)\]')
sq_instance_pat = re.compile(r'SQ instance \[([^]]+)\] SQL Query \[(.*)\]', re.DOTALL)
sq_reader_pat = re.compile(
    r'Read \[(\d+)\] rows, read \[(\d+)\] error rows for source table \[(\w+)\] instance name \[(\w+)\]')
param_pat = re.compile(
    r'Use default value \[([^]]+)\] for mapping parameter:\[([^]]+)\]', re.DOTALL)
param_wf_pat = re.compile(
    r'Use override value \[([^]]+)\] for user-defined workflow/worklet variable:\[([^]]+)\]', re.DOTALL)
param_pers_pat = re.compile(
    r'Use persisted repository value \[([^]]+)\] for mapping variable:\[([^]]+)\]', re.DOTALL)
wf_run_info_pat = re.compile(
    r'Workflow: \[([^]]+)\] Run Instance Name: \[([^]]*)\] Run Id: \[(\d+)\]', re.DOTALL)


def parse_log_message(message):

    def find_first_matching(*patterns):
        found = False
        ret = []
        for p in patterns:
            if found:
                ret.append([])
                continue
            result = p.findall(message)
            if len(result) > 0:
                ret.append(result)
                found=True
            else:
                ret.append([])
        return found, ret

    matched, ( src_trg_type, lookup, lookup_trans,  targets, sq_instances, sq_reader, params, params_wf, params_pers, wf_run_info) = find_first_matching(
                                                                                                                src_tgt_summary_pat,
                                                                                                                lookup_pat,
                                                                                                                lookup_trans_pat,
                                                                                                                target_pat,
                                                                                                                sq_instance_pat,
                                                                                                                sq_reader_pat,
                                                                                                                param_pat,
                                                                                                                param_wf_pat,
                                                                                                                param_pers_pat,
                                                                                                                wf_run_info_pat)

    ParsedLogEntry = namedtuple("ParsedLogEntry",
                                "matched src_trg_type lookup lookup_trans targets sq_instances sq_reader params params_wf params_pers wf_run_info")

    return ParsedLogEntry(matched, src_trg_type, lookup, lookup_trans, targets, sq_instances, sq_reader, params, params_wf, params_pers, wf_run_info)
