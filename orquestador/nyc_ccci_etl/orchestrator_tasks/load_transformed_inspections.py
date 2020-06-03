import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.orchestrator_tasks.feature_engineering_validation_metadata import FeatureEngineeringValidationMetadata

from nyc_ccci_etl.etl.inspections_transformer import InspectionsTransformer
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class LoadTransformedInspections(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    
    host, database, user, password = get_database_connection_parameters()
    table = "transformed.inspections"

    def requires(self):
        return FeatureEngineeringValidationMetadata(self.year, self.month, self.day)
    
    def run(self):
        transform_inspections = InspectionsTransformer(self.year, self.month, self.day)
        self._rows, self.columns = transform_inspections.execute()

        super().run()

    def rows(self):        
        for element in self._rows:
            yield element