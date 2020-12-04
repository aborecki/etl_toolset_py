
def set_assignee_for_subissues_of_issue(jira_conn, issue_id, assignee):
    iss = jira_conn.get_issue(issue_id)
    for s in iss.get_sub_issues():
        s.set_assignee_name(assignee)
        jira_conn.update_issue(s)