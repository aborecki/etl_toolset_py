import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from bectools.bec.dashboard.app import app
from bectools.bec.dashboard.apps import etl_dashboard_job_deps, etl_dashboard_workflows


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Link('Navigate to etl_dashboard_job_deps', href='/apps/etl_dashboard_job_deps'),
    html.Br(),
    dcc.Link('Navigate to etl_dashboard_workflows', href='/apps/etl_dashboard_workflows'),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/etl_dashboard_workflows':
        return etl_dashboard_workflows.layout
    elif pathname == '/apps/etl_dashboard_job_deps':
        return etl_dashboard_job_deps.layout
    else:
        return ''


if __name__ == '__main__':
    app.run_server(debug=True)