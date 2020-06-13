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


def get_opc_dependencies_dataset( opc_jobname='%'):
    return datasources.get_opc_dependencies_dataset( opc_jobname)

df=get_opc_dependencies_dataset(opc_jobname="XXXXXX")
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Input(
        id="input_opc_jobname_name"
    ),
    html.Button("Submit", id='submit', n_clicks=0),
    dash_table.DataTable(
        id='datatable-opc_deps',
        columns=[
            {"name": i, "id": i, "deletable": True, "selectable": True} for i in df.columns
        ],
        data=df.to_dict('records'),
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

@app.callback(Output('datatable-opc_deps', 'data' ),[Input('submit',"n_clicks")], [State('input_opc_jobname_name', 'value')])
def update_selection(dummy,wf_name):
    df = get_opc_dependencies_dataset(wf_name)
    return df.to_dict('records')


if __name__ == '__main__':
    app.run_server(debug=True)
