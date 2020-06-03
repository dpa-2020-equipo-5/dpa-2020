from marbles.core import TestCase
from marbles.core.marbles import ContextualAssertionError
from nyc_ccci_etl.etl.inspections_extractor import InspectionsExtractor
from datetime import datetime
from nyc_ccci_etl.utils.is_date_valid import is_date_valid

def trim(_str):
    return _str.replace('\n', '\t').strip()
    
class TestInspectionsExtractor(TestCase):

    def test_extraction_date_is_valid(self, year, month, day):
        note = "{}-{}-{} debe ser fecha válida y no futura".format(year, month, day)
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.assertEqual(is_date_valid(year, month, day), True, note=note)
            return {"test":"test_extraction_date_is_valid", "status":"passed", "note": note, "ran_at": ran_at}
        except ContextualAssertionError as e:
            return{"test":"test_extraction_date_is_valid", "status":"failed", "note": trim(e.note), "ran_at": ran_at}  
    
    def test_extaction_is_not_empty(self, year, month, day):
        extractor = InspectionsExtractor(year, month, day)
        result = extractor.execute()
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.assertGreater(len(result), 0, note="Debe haber por lo menos 1 inspección")
            return {"test":"test_extaction_is_not_empty", "status":"passed", "note": "Debe haber por lo menos 1 inspección", "ran_at": ran_at}
        except ContextualAssertionError as e:
            return {"test":"test_extaction_is_not_empty", "status":"failed", "note": trim(e.note), "ran_at": ran_at}

    def test_extraction_is_json(self, year, month, day):
        extractor = InspectionsExtractor(year, month, day)
        result = extractor.execute()
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.assertGreater(len(result), 0, note="Debe haber por lo menos 1 inspección")
            self.assertIsInstance(result, list, note="El resulto de la extracción es una lista")
            for item in result:
                self.assertEqual(str(item)[0], '{', note="El primer caracter de cada elemento de la lista es una llave abierta")
                self.assertEqual(str(item)[-1], '}', note="El primer caracter de cada elemento de la lista es una llave cerrada")
            return {"test":"test_extraction_is_json","status":"passed", "note": "El resultado de la extracción es una lista de diccionarios", "ran_at": ran_at}
        except ContextualAssertionError as e:
            return {"test":"test_extraction_is_json","status":"failed", "note": trim(e.note), "ran_at": ran_at}

    def test_inspection_date_should_match_params_date(self, year, month, day):
        extractor = InspectionsExtractor(year, month, day)
        result = extractor.execute()
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.assertGreater(len(result), 0, note="Debe haber por lo menos 1 inspección")
            for item in result:
                self.assertEqual(datetime.strptime(item['inspectiondate'], '%Y-%m-%dT00:00:00.000'), datetime(year, month, day), note="La fecha del request {}-{}-{} == fechainspección {}".format(year, month, day, item['inspectiondate']))
            return {"test":"test_inspection_date_should_match_params_date","status":"passed", "note": "Fecha del rerquest {}-{}-{} == las fechas de todas las inspecciones".format(year, month, day ), "ran_at": ran_at}
        except ContextualAssertionError as e:
            return {"test":"test_inspection_date_should_match_params_date","status":"failed", "note": trim(e.note), "ran_at": ran_at}