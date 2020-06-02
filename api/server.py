import re
import configparser

from flask import Flask
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
api = Api(app)

config = configparser.ConfigParser()
#config.read_file(open('/var/www/html/dpa-2020/api/config.ini'))
config.read_file(open('./config.txt'))
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

def get_borough(arr):
    boroughs = ("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")
    indx = [i for i, x in enumerate(arr) if x == '1'][0]
    return boroughs[indx]

def get_predictions(date):
    db_result = db.engine.execute('SELECT a.*, b.centername, b.childcaretype,b.borough_bronx, b.borough_brooklyn, b.borough_manhattan, b.borough_queens, b.borough_staten_island FROM predictions.predictions a join transformed.centers b on a.center_id = b.dc_id '
                                  ' WHERE DATE(a.date) = \'{}\''
                                  ' ORDER BY a.priority'.format(date)).fetchall()
    result = []
    for prediction in db_result:
        result.append({
            'centerId': prediction[0],
            'centerName':prediction[5],
            'childcareType': prediction[6],
            'borough': get_borough(prediction[7:]),
            'probability':float(prediction[1]),
            'probability_str': str(round(float(prediction[1]) * 100, 3) )  + "%" ,
            'priority': prediction[3]
        })

    return result


def get_inspections(date):
    db_result = db.engine.execute('SELECT * FROM transformed.inspections as INSP'
                                  ' WHERE DATE(INSP.inspectiondate) >= \'{}\''
                                  ' ORDER BY INSP.inspectiondate'.format(date)).fetchall()
    result = []
    for prediction in db_result:
        result.append({
            'center_id.append({': prediction[0],
            'inspectiondate': prediction[1],
            'regulationsummary': prediction[2],
            'healthcodesubsection': prediction[3],
            'violationstatus': prediction[4],
            'borough': prediction[5],
            'reason': prediction[6],
            'result_1_passed_inspection': prediction[7],
            'result_1_passed_inspection_with_no_violations': prediction[8],
            'result_1_previously_cited_violations_corrected': prediction[9],
            'result_1_previously_closed_program_re_opened': prediction[10],
            'result_1_reinspection_not_required': prediction[1],
            'result_1_reinspection_required': prediction[12],
            'result_2_nr': prediction[13],
            'result_2_fines_pending': prediction[14],
            'result_2_program_closed': prediction[15],
            'result_2_violations_corrected_at_time_of_inspection': prediction[16],
            'inspection_year': prediction[17],
            'inspection_month': prediction[18],
            'inspection_day_name': prediction[19],
            'violationcategory_critical': prediction[20],
            'violationcategory_general': prediction[21],
            'violationcategory_nan': prediction[22],
            'violationcategory_public_health_hazard': prediction[23],
            'dias_ultima_inspeccion': prediction[24],
            'violaciones_hist_salud_publica': prediction[25],
            'violaciones_2019_salud_publica': prediction[26],
            'violaciones_hist_criticas': prediction[27],
            'violaciones_2019_criticas': prediction[28],
            'ratio_violaciones_hist': prediction[29],
            'ratio_violaciones_2019': prediction[30],
            'prom_violaciones_hist_borough': prediction[31],
            'prom_violaciones_2019_borough': prediction[32],
            'ratio_violaciones_hist_sp': prediction[33],
            'ratio_violaciones_2019_sp': prediction[34],
            'ratio_violaciones_hist_criticas': prediction[35],
            'ratio_violaciones_2019_criticas': prediction[36],
        })

    return result


def get_model_parameters():
    db_result = db.engine.execute('SELECT * FROM modeling.model_parameters').fetchall()

    result = []
    for prediction in db_result:
        result.append({
            'task_id': prediction[0],
            'created_at': str(prediction[1]),
            'score': prediction[2],
            'n_estimators': prediction[3],
            'bootstrap': prediction[4],
            'class_weight': prediction[5],
            'max_depth': prediction[6],
            'criterion': prediction[7],
        })

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


class InspectionsByDate(Resource):

    def get(self, date):
        if not valid_date(date):
            return 'Invalid date supplied. Please follow the pattern: yyyy-MM-dd', 400

        centers = get_inspections(date)
        if len(centers) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'centers': centers}


api.add_resource(InspectionsByDate, '/inspection/<date>')


class ModelParameters(Resource):

    def get(self):
        parameters = get_model_parameters()

        if len(parameters) == 0:
            return 'No model parameters available', 404

        return parameters


api.add_resource(ModelParameters, '/model_parameter')


if __name__ == '__main__':
    app.run(debug=True, port=3000)
