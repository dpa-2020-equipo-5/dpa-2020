import luigi
from .predictions_validations.predictions_columns_validation import PredictionsColumnsValidation
from .predictions_validations.at_least_one_center_validation import AtLeastOneCenterValidation

from nyc_ccci_etl.orchestrator_tasks.feature_engineering_validation_metadata import FeatureEngineeringValidationMetadata
from nyc_ccci_etl.orchestrator_tasks.load_bias_fairness_metadata import LoadBiasFairnessMetadata
class PredictionsValidationMetadata(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    matrix_uuid = luigi.Parameter()
    pipeline_type=luigi.Parameter()
    _complete = False
    def requires(self):
        return(
            FeatureEngineeringValidationMetadata(self.year, self.month, self.day), 
            LoadBiasFairnessMetadata(self.year, self.month, self.day, self.pipeline_type)
        )
    def run(self):
        yield (
            PredictionsColumnsValidation(self.year, self.month, self.day, self.matrix_uuid),
            AtLeastOneCenterValidation(self.year, self.month, self.day, self.matrix_uuid)
        )
        self._complete = True

    def complete(self):
        return self._complete
    
    