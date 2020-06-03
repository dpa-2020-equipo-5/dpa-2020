import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.etl.centers_updater import CentersUpdater
from .load_clean_inspections_metadata import LoadCleanInspectionsMetadata
from .feature_engineering_validation_metadata import FeatureEngineeringValidationMetadata

class UpdateCenters(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return FeatureEngineeringValidationMetadata(self.year, self.month, self.day)

    host, database, user, password = get_database_connection_parameters()
    table = "transformed.centers"

    def run(self):
        centers_updater = CentersUpdater(self.year, self.month, self.day)
        self.rs, self.columns = centers_updater.execute()
        super().run()

    def rows(self):        
        for element in self.rs:
            yield element