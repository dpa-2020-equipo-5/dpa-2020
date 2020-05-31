import re
from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

date_regex = re.compile(r"^[0-9]{4}-([0]?[1-9]|1[0-2])$")


def valid_date(date):
    return bool(date_regex.match(date))


def get_predictions(date):
    print(date[5])
    if date[5] == '0':
        return []

    return [{
        'center_id': '123xyz',
        'ranking': '1',
    }]


class Prediction(Resource):

    def get(self, date):
        if not valid_date(date):
            return 'Invalid date supplied. Please follow the pattern: yyyy-MM', 400
        centers = get_predictions(date)

        if len(centers) == 0:
            return 'No data available for {}'.format(date), 404

        return {"date": date, 'centers': centers}


api.add_resource(Prediction, '/prediction/<date>')

if __name__ == '__main__':
    app.run(debug=True)
