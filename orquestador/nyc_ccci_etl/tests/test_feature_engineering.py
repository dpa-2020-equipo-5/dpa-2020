from marbles.core import TestCase
from marbles.core.marbles import ContextualAssertionError
from nyc_ccci_etl.etl.inspections_transformer import InspectionsTransformer
from datetime import datetime
import pandas as pd

def trim(_str):
    return _str.replace('\n', '').replace('\t', '').strip()
    
class TestFeatureEngineering(TestCase):
    def test_columns_one_hot_encoding(self, year, month, day):
        transformer = InspectionsTransformer(year, day, month)
        transformer.execute()
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cols = ["violationcategory_public_health_hazard", "violationcategory_critical", "violationcategory_general"]
        try:
            for c in cols:
                self.assertIn(c, transformer.output_table.columns, note="{} deben estar presentes".format(",".join(cols)))
            return {"test":"test_columns_one_hot_encoding","status":"passed", "note":  "Las columnas {} están presentes".format(",".join(cols)), "ran_at": ran_at}
        except ContextualAssertionError as e:
            return {"test":"test_columns_one_hot_encoding","status":"failed", "note": trim(e.note), "ran_at": ran_at}
    
    def test_transformed_inspections_match_date(self, year, month, day):
        transformer = InspectionsTransformer(year, month, day)
        transformer.execute()
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            self.assertLessEqual( len(transformer.output_table['inspectiondate'].unique()), 1, note="{} fechas únicas en las transformacciones. Todas las inspecciones deben ser de la misma fecha".format(len(transformer.output_table['inspectiondate'].unique())) ) 
            return {"test":"test_transformed_inspections_match_date","status":"passed", "note": "Todas las inspeccciones tienen la misma fecha".format(year, month, day ), "ran_at": ran_at}
        except ContextualAssertionError as e:
            return {"test":"test_inspection_date_should_match_params_date","status":"failed", "note": trim(e.note), "ran_at": ran_at}