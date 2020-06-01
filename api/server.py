import re
import configparser

from flask import Flask
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
api = Api(app)

config = configparser.ConfigParser()
config.read('config.txt')
db_user = config['db']['user']
db_pass = config['db']['pass']
db_url = config['db']['url']
db_name = config['db']['db_name']
DB_URL = 'postgresql+psycopg2://{user}:{pw}@{url}/{db}'.format(user=db_user, pw=db_pass, url=db_url, db=db_name)
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # silence the deprecation warning
db = SQLAlchemy(app)

date_regex = re.compile(r"^[0-9]{4}-([0]?[1-9]|1[0-2])-[0-9]{1,2}$")


def valid_date(date):
    return bool(date_regex.match(date))


def get_last_prediction_date():
    result = db.engine.execute('SELECT DATE(MAX(date)) FROM predictions.predictions').first()
    return str(result[0])


def get_predictions(date):
    db_result = db.engine.execute('SELECT * FROM predictions.predictions as PRED'
                                  ' WHERE DATE(PRED.date) = \'{}\''
                                  ' ORDER BY PRED.priority'.format(date)).fetchall()
    result = []
    for prediction in db_result:
        result.append({'centerId': prediction[0], 'ranking': prediction[3]})

    return result


class Prediction(Resource):

    def get(self):
        date = get_last_prediction_date()
        centers = get_predictions(date)

        if len(centers) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'centers': centers}


api.add_resource(Prediction, '/prediction/')


class PredictionWithParameters(Resource):

    def get(self, date):
        if not valid_date(date):
            return 'Invalid date supplied. Please follow the pattern: yyyy-MM-dd', 400

        centers = get_predictions(date)
        if len(centers) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'centers': centers}


api.add_resource(PredictionWithParameters, '/prediction/<date>')


class PredictionsModel(db.Model):
    __tablename__ = 'predictions.predictions'

    center_id = db.Column(db.String(200), primary_key=True)
    probability = db.Column(db.Float)
    date = db.Column(db.DateTime)
    priority = db.Column(db.Integer)
    matrix_uuid = db.Column(db.String(200))


if __name__ == '__main__':
    app.run(debug=True)
