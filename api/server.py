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
            'centerName':prediction[5].replace("summer", ""),
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
            'center_id': prediction[0],
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


def get_aequitas_groups(date):
    db_result = db.engine.execute('SELECT * FROM aequitas.groups as AEQ'
                                  ' WHERE DATE(AEQ.date) = \'{}\''.format(date)).fetchall()

    result = []
    for prediction in db_result:
        result.append({
            'model_id': prediction[0],
            'score_threshold': prediction[1],
            'k': prediction[2],
            'attribute_name': prediction[3],
            'attribute_value': prediction[4],
            'pp': prediction[5],
            'pn': prediction[6],
            'fp': prediction[7],
            'fn': prediction[8],
            'tn': prediction[9],
            'tp': prediction[10],
            'group_label_pos': prediction[11],
            'group_label_neg': prediction[12],
            'group_size': prediction[13],
            'total_entities': prediction[14],
            'date': str(prediction[15]),
        })

    return result


def get_aequitas_fairness(date):
    db_result = db.engine.execute('SELECT * FROM aequitas.fairness as AEQ'
                                  ' WHERE DATE(AEQ.date) = \'{}\''.format(date)).fetchall()

    result = []
    for prediction in db_result:
        result.append({
            'model_id': prediction[0],
            'score_threshold': prediction[1],
            'k': prediction[2],
            'attribute_name': prediction[3],
            'attribute_value': prediction[4],
            'tpr': prediction[5],
            'tnr': prediction[6],
            'forr': prediction[7],
            'fdr': prediction[8],
            'fpr': prediction[9],
            'fnr': prediction[10],
            'npv': prediction[11],
            'precision': prediction[12],
            'pp': prediction[13],
            'pn': prediction[14],
            'ppr': prediction[15],
            'pprev': prediction[16],
            'fp': prediction[17],
            'fn': prediction[18],
            'tn': prediction[19],
            'tp': prediction[20],
            'group_label_pos': prediction[21],
            'group_label_neg': prediction[22],
            'group_size': prediction[23],
            'total_entities': prediction[24],
            'prev': prediction[25],
            'ppr_disparity': prediction[26],
            'pprev_disparity': prediction[27],
            'precision_disparity': prediction[28],
            'fdr_disparity': prediction[29],
            'forr_disparity': prediction[30],
            'fpr_disparity': prediction[31],
            'fnr_disparity': prediction[32],
            'tpr_disparity': prediction[33],
            'tnr_disparity': prediction[34],
            'npv_disparity': prediction[35],
            'ppr_ref_group_value': prediction[36],
            'pprev_ref_group_value': prediction[37],
            'precision_ref_group_value': prediction[38],
            'fdr_ref_group_value': prediction[39],
            'forr_ref_group_value': prediction[40],
            'fpr_ref_group_value': prediction[41],
            'fnr_ref_group_value': prediction[42],
            'tpr_ref_group_value': prediction[43],
            'tnr_ref_group_value': prediction[44],
            'npv_ref_group_value': prediction[45],
            'statistical_parity': prediction[46],
            'impact_parity': prediction[47],
            'fdr_parity': prediction[48],
            'fpr_parity': prediction[49],
            'for_parity': prediction[50],
            'fnr_parity': prediction[51],
            'tpr_parity': prediction[52],
            'tnr_parity': prediction[53],
            'npv_parity': prediction[54],
            'precision_parity': prediction[55],
            'typei_parity': prediction[56],
            'typeii_parity': prediction[57],
            'equalized_odds': prediction[58],
            'unsupervised_fairness': prediction[59],
            'supervised_fairness': prediction[60],
            'date': str(prediction[61]),
        })

    return result


def get_aequitas_bias(date):
    db_result = db.engine.execute('SELECT * FROM aequitas.bias as AEQ'
                                  ' WHERE DATE(AEQ.date) = \'{}\''.format(date)).fetchall()

    result = []
    for prediction in db_result:
        result.append({
            'model_id character': prediction[0],
            'score_threshold': prediction[1],
            'k character': prediction[2],
            'attribute_name': prediction[3],
            'attribute_value': prediction[4],
            'tpr': prediction[5],
            'tnr': prediction[6],
            'forr': prediction[7],
            'fdr': prediction[8],
            'fpr': prediction[9],
            'fnr': prediction[10],
            'npv': prediction[11],
            'precision': prediction[12],
            'pp': prediction[13],
            'pn': prediction[14],
            'ppr': prediction[15],
            'pprev': prediction[16],
            'fp': prediction[17],
            'fn': prediction[18],
            'tn': prediction[19],
            'tp': prediction[20],
            'group_label_pos': prediction[21],
            'group_label_neg': prediction[22],
            'group_size': prediction[23],
            'total_entities': prediction[24],
            'prev': prediction[25],
            'ppr_disparity': prediction[26],
            'pprev_disparity': prediction[27],
            'precision_disparity': prediction[28],
            'fdr_disparity': prediction[29],
            'forr_disparity': prediction[30],
            'fpr_disparity': prediction[31],
            'fnr_disparity': prediction[32],
            'tpr_disparity': prediction[33],
            'tnr_disparity': prediction[34],
            'npv_disparity': prediction[35],
            'ppr_ref_group_value': prediction[36],
            'pprev_ref_group_value': prediction[37],
            'precision_ref_group_value': prediction[38],
            'fdr_ref_group_value': prediction[39],
            'forr_ref_group_value': prediction[40],
            'fpr_ref_group_value': prediction[41],
            'fnr_ref_group_value': prediction[42],
            'tpr_ref_group_value': prediction[43],
            'tnr_ref_group_value': prediction[44],
            'npv_ref_group_value': prediction[45],
            'date': str(prediction[46]),
        })

    return result


# Endpoints definitions

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


class AequitasGroups(Resource):

    def get(self, date):
        if not valid_date(date):
            return 'Invalid date supplied. Please follow the pattern: yyyy-MM-dd', 400

        groups = get_aequitas_groups(date)
        if len(groups) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'aequitas_groups': groups}


api.add_resource(AequitasGroups, '/aequitas/groups/<date>')


class AequitasFairness(Resource):

    def get(self, date):
        if not valid_date(date):
            return 'Invalid date supplied. Please follow the pattern: yyyy-MM-dd', 400

        fairness = get_aequitas_fairness(date)
        if len(fairness) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'aequitas_fairness': fairness}


api.add_resource(AequitasFairness, '/aequitas/fairness/<date>')


class AequitasBias(Resource):

    def get(self, date):
        if not valid_date(date):
            return 'Invalid date supplied. Please follow the pattern: yyyy-MM-dd', 400

        bias = get_aequitas_bias(date)
        if len(bias) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'aequitas_bias': bias}


api.add_resource(AequitasBias, '/aequitas/bias/<date>')


if __name__ == '__main__':
    app.run(debug=True, port=3000)
