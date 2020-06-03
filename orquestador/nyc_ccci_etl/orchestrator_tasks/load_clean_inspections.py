import luigi
from luigi.contrib.postgres import CopyToTable
from nyc_ccci_etl.orchestrator_tasks.extraction_validation_metadata import ExtractionValidationMetadata
from nyc_ccci_etl.etl.inspections_extractor import InspectionsExtractor
from nyc_ccci_etl.etl.inspections_cleaner import InspectionsCleaner
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class LoadCleanInspections(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    
    def requires(self):
        return ExtractionValidationMetadata(self.year, self.month, self.day)

    host, database, user, password = get_database_connection_parameters()
    table = "clean.inspections"

    def run(self):
        etl_extraction = InspectionsExtractor(self.year, self.month, self.day)
        inspections_json_data = etl_extraction.execute()
        cleaner = InspectionsCleaner(inspections_json_data)
        self._rows, self._columns = cleaner.execute()
        self.columns = [ (c, 'VARCHAR') for c in self._columns]
        super().run()
    
    
    def rows(self):        
        for element in self._rows:
            yield element