import dash
import dash_core_components as dcc
import dash_table
import dash_html_components as html
import flask
import os
from random import randint
import pandas as pd
import requests
from datetime import datetime
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']



server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
app = dash.Dash(__name__, server=server,  external_stylesheets=external_stylesheets)

app.title = 'NYC Childcare Centers inspection predictions'
app.head = [
    html.Link(
        href='https://github.com/dpa-2020-equipo-5/dpa-2020/tree/master/nyccci_dashboard/favicon.ico',
        rel='icon'
    ),
]

#r = requests.get("http://localhost:3000/prediction/")
r = requests.get("http://18.208.188.16/prediction")
df = pd.json_normalize(r.json(), 'centers')
date_split = r.json()['date'].split('-')
d = datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]))
date_label = "Fecha de las predicciones: "+d.strftime(format="%Y-%m-%d")
borough_count = df['borough'].value_counts().reset_index().rename(columns={'index':'borough','borough':'total'})
df['fullName'] = df['centerName'].apply(lambda x: x.replace('_', ' ').title())
type_count = df['childcareType'].value_counts().reset_index().rename(columns={'index':'childcareType','childcareType':'total'})
colors = [
    '#1f77b4',  # muted blue
    '#ff7f0e',  # safety orange
    '#2ca02c',  # cooked asparagus green
    '#d62728',  # brick red
    '#9467bd',  # muted purple
    '#8c564b',  # chestnut brown
    '#e377c2',  # raspberry yogurt pink
    '#7f7f7f',  # middle gray
    '#bcbd22',  # curry yellow-green
    '#17becf'   # blue-teal
]


app.layout = html.Div(children=[
    html.H1(children='New York City Childcare Centers Inspections'),
    html.H4(children='Centros con mayor probabilidad de presentar una violación de salud pública'),
    html.P(children=date_label),
    dcc.DatePickerSingle(
        id='my-date-picker-range',
        min_date_allowed=datetime(1995, 8, 5),
        max_date_allowed=datetime(2020, 6, 1),
        initial_visible_month=datetime(2020, 1, 1),
        style={
            "marginBottom":"15px"
        }
    ),
    html.Div(id='output-container-date-picker-range'),
    dash_table.DataTable(
        id='centersTable',
        columns=[{"name": i, "id": i} for i in df[['priority','fullName', 'probability_str', 'borough']].columns],
        
        data=df[['priority','fullName', 'probability_str', 'borough']].to_dict('records'),
        style_table={
            'width': '100%',
        },
        style_cell={
                'textAlign': 'left'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }
        ],
        style_header={
        'backgroundColor': 'rgb(230, 230, 230)',
        'fontWeight': 'bold'
        },
        page_size=10
    ),
    html.Div([
dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {
                    'x': borough_count['borough'].values,
                    'y': borough_count['total'].values,
                    'type': 'bar',
                    'name': 'Borough',
                    'marker': {'color': colors}
                },
            ],
            'layout': {
                'title': 'Distritos de las guarderías'
            },
        },
        style={
            'marginTop':'50px',
        },
        className="five columns"
    ),
    dcc.Graph(
        id='childcare-type-graph',
        figure={
            'data': [
                {
                    'x': [x.replace('_', ' ') for x in type_count['childcareType'].values],
                    'y': type_count['total'].values,
                    'type': 'bar',
                    'name': 'childcareType',
                    'marker': {'color': colors}
                }
            ],
            'layout': {
                'title': 'Tipo de guardería'
            }
        },
        className="seven columns"
    )
    ], className="row"),
])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})
# Run the Dash app
if __name__ == '__main__':
    app.server.run(debug=True, threaded=True)