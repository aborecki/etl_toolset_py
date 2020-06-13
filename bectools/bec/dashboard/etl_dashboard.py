import dash
import dash_core_components as dcc
import dash_html_components as html
from bectools import connectors as _
from dash.dependencies import Input, Output
import cufflinks as cf
cf.go_offline()

import pandas as pd

wf_names = list(_.DB.PROD.SQL_PC_REP.DS.D00000PD10_PWC_REP_PROD.run_query_pandas_dataframe(f"""
    SELECT  distinct left(workflow_name, len(workflow_name)-charindex('_', reverse(workflow_name)) ) workflow_name
    FROM  [BEC_TASK_INST_RUN]
    where START_TIME > '2019-01-01' and run_err_code =0 and task_type_name = 'Session'
""")["workflow_name"])

def get_task_names(wf_name):
    return list(_.DB.PROD.SQL_PC_REP.DS.D00000PD10_PWC_REP_PROD.run_query_pandas_dataframe(f"""
    SELECT  distinct task_name
    FROM  [BEC_TASK_INST_RUN]
    where START_TIME > '2019-01-01' and run_err_code =0 and task_type_name = 'Session'
    and 
        ( 
        upper(WORKFLOW_NAME) like upper('{wf_name}%')  
        )
""")["task_name"])

def get_executions(dropdown_values, wf_name):
    cond=",".join([f"'{i}'" for i in dropdown_values])
    executions = _.DB.SQLPROD_PC_REP.DS.D00000PD10_PWC_REP_PROD.run_query_pandas_dataframe(f"""
    select cast(start_time as date) start_time, workflow_name, task_name, sum(targ_success_rows) targ_success_rows
    from 
    (    
        SELECT  start_time, 
        left(workflow_name, len(workflow_name)-charindex('_', reverse(workflow_name)) ) workflow_name,
        task_name,
        targ_success_rows
        FROM  [BEC_TASK_INST_RUN] where
        task_name in ({cond})
        and START_TIME > '2020-01-01' and run_err_code =0 and task_type_name = 'Session'
        and workflow_run_id not in (select workflow_run_id   FROM  [BEC_TASK_INST_RUN]
          where START_TIME > '2020-01-01' and run_err_code <>0 
        and 
        ( 
        WORKFLOW_NAME like '{wf_name}%'
        )
        )
        ) a where workflow_name like '{wf_name}%' group by cast(start_time as date), workflow_name, task_name
        order by 1 desc
    """)
    return executions

app = dash.Dash()
task_names=get_task_names(wf_names[0])
app.layout = html.Div([
    html.H1('BEC_TASK_INST_RUN'),
    dcc.Dropdown(
        id='wf_name',
        options=[{'label': t, 'value': t} for t in wf_names],
        value=wf_names[0],
        multi=False,
    ),
    dcc.Dropdown(
        id='my-dropdown',
        options=[ {'label': t, 'value': t} for t in task_names],
        value=task_names,
        multi=True,
    ),
    dcc.Graph(id='my-graph')
])


@app.callback(Output('my-dropdown', 'options'), [Input('wf_name', 'value')])
def update_task_names(wf_name):
    task_names = get_task_names(wf_name)
    return [ {'label': t, 'value': t} for t in task_names]

@app.callback(Output('my-graph', 'figure'), [Input('my-dropdown', 'value'), Input('wf_name', 'value')])
def plot_time_series(dropdown_value, wf_name):
    dat_x = get_executions(dropdown_value, wf_name)
    dat_x=dat_x.pivot(index="start_time", columns="task_name", values="targ_success_rows")
    dat_x.columns = dropdown_value
    return dat_x.iplot(kind="bar", asFigure=True)


if __name__ == '__main__':
    app.run_server(debug=False)
