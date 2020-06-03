import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.orchestrator_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.orchestrator_tasks.load_update_centers_metadata import LoadUpdateCentersMetadata

from nyc_ccci_etl.model.random_forest_grid_search import RandomForestGridSearch
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

from datetime import datetime

class ModelMetadata(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return (
            LoadTransformedInspectionsMetadata(self.year, self.month, self.day), LoadUpdateCentersMetadata(self.year, self.month, self.day)
        )

    host, database, user, password = get_database_connection_parameters()
    table = "modeling.model_parameters"
    schema = "modeling"

    def run(self):
        self.model_params, self.score = RandomForestGridSearch.find_best_params()
        self.columns = [
            ('task_id', 'varchar'),
            ('created_at', 'timestamp'),
            ('score', 'float'),
            ('n_estimators', 'integer'),
            ('bootstrap', 'boolean'),
            ('class_weight', 'varchar'),
            ('max_depth', 'integer'),
            ('criterion', 'varchar')
        ]
        super().run()

    def rows(self):
        yield ("{}{}{}".format(str(self.year), str(self.month), str(self.day)), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), self.score, self.model_params['n_estimators'], self.model_params['bootstrap'], self.model_params['class_weight'], self.model_params['max_depth'], self.model_params['criterion'])
