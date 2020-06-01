import dash
import dash_core_components as dcc
import dash_table
import dash_html_components as html
import flask
import os
from random import randint
import pandas as pd
import requests
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
app = dash.Dash(__name__, server=server,  external_stylesheets=external_stylesheets)

r = requests.get("http://18.208.188.16/prediction/")
df = pd.json_normalize(r.json(), 'centers')
print(df)
app.layout = html.Div(children=[
    html.H1(children='New York City Childcare Centers Inspections'),

    html.Div(children='''
        Centros con mayor probabilidad de presentar una violación de salud pública
    '''),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
    ),
    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )

], style={'columnCount': 2})


# Run the Dash app
if __name__ == '__main__':
    app.server.run(debug=True, threaded=True)