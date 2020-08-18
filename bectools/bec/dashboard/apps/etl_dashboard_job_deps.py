import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from bectools import connectors as _
from dash.dependencies import Input, Output
from dash.dependencies import Input, Output
from dash.dependencies import Input, Output, State
import cufflinks as cf
cf.go_offline()

import pandas as pd
from pyetltools import connector
from  bectools import connectors as con
from  bectools import connectors as con
from bectools.bec import datasources

from bectools.bec.dashboard.app import app


df_succ=datasources.get_opc_dependencies_succ_pd_df(condition="1=0")
df_pred=datasources.get_opc_dependencies_pred_pd_df(condition="1=0")
layout = html.Div([
    dcc.Input(
        id="input_opc_jobname_name"
    ),
    html.Button("Submit", id='submit_opc_deps', n_clicks=0),
    dash_table.DataTable(
        id='datatable-opc_deps_succ',
        columns=[
            {"name": i, "id": i, "deletable": True, "selectable": True} for i in df_succ.columns
        ],
        data=df_succ.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        row_deletable=True,
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 100,
    ),
    dash_table.DataTable(
        id='datatable-opc_deps_pred',
        columns=[
            {"name": i, "id": i, "deletable": True, "selectable": True} for i in df_pred.columns
        ],
        data=df_pred.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        row_deletable=True,
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current=0,
        page_size=100,
    ),
    html.Div(id='datatable-interactivity-container')
])

@app.callback(
    Output('datatable-opc_deps', 'style_data_conditional'),
    [Input('datatable-opc_deps', 'selected_columns')]
)
def update_styles(selected_columns):
    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]

@app.callback(Output('datatable-opc_deps_pred', 'data' ),[Input('submit_opc_deps',"n_clicks")], [State('input_opc_jobname_name', 'value')])
def update_selection(dummy,opc):
    df = datasources.get_opc_dependencies_pred_pd_df(opc)
    return df.to_dict('records')

@app.callback(Output('datatable-opc_deps_succ', 'data' ),[Input('submit_opc_deps',"n_clicks")], [State('input_opc_jobname_name', 'value')])
def update_selection2(dummy,opc):
    df = datasources.get_opc_dependencies_succ_pd_df(opc)
    return df.to_dict('records')

