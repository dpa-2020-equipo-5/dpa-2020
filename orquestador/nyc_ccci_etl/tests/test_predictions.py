from marbles.core import TestCase
from marbles.core.marbles import ContextualAssertionError
from nyc_ccci_etl.predict.predictions_creator import PredictionsCreator
from datetime import datetime
import pandas as pd

def trim(_str):
    return _str.replace('\n', '').replace('\t', '').strip()
    
class TestPredictions(TestCase):
    def test_predictions_have_correct_columns(self, model, year, month, day, matrix_uuid):
        predictor = PredictionsCreator(model, year, month, day, matrix_uuid)
        predictor.create_predictions()
        predictions = predictor.output_table
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            for col in predictions:
                self.assertTrue(col in ['center_id', 'probability', 'date', 'priority', 'matrix_uuid'], note="Las predicciones deben tener las columnas center_id, probability, date, priority, matrix_uuid.")
            
            return {
                "test":"test_predictions_have_correct_columns",
                "status":"passed",
                "note" : "Todas las inspeccciones tienen las columnas  center_id, probability, date, priority, matrix_uuid", 
                "ran_at": ran_at
            }
        except ContextualAssertionError as e:
            return {"test":"test_predictions_have_correct_columns","status":"failed", "note": trim(e.note), "ran_at": ran_at}

    def test_at_least_one_center(self, model, year, month, day, matrix_uuid):
        predictor = PredictionsCreator(model, year, month, day, matrix_uuid)
        predictor.create_predictions()
        predictions = predictor.output_table
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.assertGreater(len(predictions), 0, note="Hay por lo menos un centro a inspeccionar.")
            return {
                "test":"test_at_least_one_center",
                "status":"passed",
                "note" : "Hay por lo menos un centro a inspeccionar.", 
                "ran_at": ran_at
            }
        except ContextualAssertionError as e:
            return {"test":"test_at_least_one_center","status":"failed", "note": trim(e.note), "ran_at": ran_at}