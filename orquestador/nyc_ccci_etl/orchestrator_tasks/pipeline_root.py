import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.orchestrator_tasks.load_bias_fairness_metadata import LoadBiasFairnessMetadata
from nyc_ccci_etl.orchestrator_tasks.load_predictions_metadata import LoadPredictionsMetadata
from nyc_ccci_etl.orchestrator_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.orchestrator_tasks.load_update_centers_metadata import LoadUpdateCentersMetadata
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from datetime import datetime

class PipelineRoot(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    pipeline_type = luigi.Parameter()

    def requires(self):
        if str(self.pipeline_type) == 'train':
            return  LoadBiasFairnessMetadata(self.year, self.month, self.day, self.pipeline_type)
        elif str(self.pipeline_type) == 'predict':
            return LoadPredictionsMetadata(self.year, self.month, self.day, self.pipeline_type)
        elif str(self.pipeline_type) == 'load':
            return (
                LoadTransformedInspectionsMetadata(self.year, self.month, self.day),
                LoadUpdateCentersMetadata(self.year, self.month, self.day)
            )

    columns = [
        ('update_id', 'text'),
        ('target_table', 'text'),
        ('inserted', 'timestamp'),
    ]
    host, database, user, password = get_database_connection_parameters()
    table = "table_updates"

    def rows(self):
        update_id = "{}_{}{}{}".format(str(self.pipeline_type),str(self.year), str(self.month), str(self.day))
        yield (update_id, "table_updates" ,datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    