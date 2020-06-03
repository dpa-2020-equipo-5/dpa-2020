import luigi
from luigi.contrib.postgres import CopyToTable
from nyc_ccci_etl.orchestrator_tasks.feature_engineering_validation_metadata import FeatureEngineeringValidationMetadata
from nyc_ccci_etl.orchestrator_tasks.predictions_validation_metadata import PredictionsValidationMetadata
from nyc_ccci_etl.orchestrator_tasks.load_bias_fairness_metadata import LoadBiasFairnessMetadata
from nyc_ccci_etl.predict.predictions_creator import PredictionsCreator
import boto3
import pickle
from io import BytesIO
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.commons.configuration import get_aws_bucket
class CreatePredictions(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    matrix_uuid = luigi.Parameter()
    pipeline_type=luigi.Parameter()
    def requires(self):
        return (
            
            PredictionsValidationMetadata(self.year, self.month, self.day, self.matrix_uuid, self.pipeline_type)
        )

    host, database, user, password = get_database_connection_parameters()
    table = "predictions.predictions"
    schema = "predictions"
    bucket = get_aws_bucket()
    def run(self):
        ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        
        latest_model = self.get_lastest_model(ses)
        
        s3_resource = ses.resource('s3')
        
        with BytesIO() as data:
            s3_resource.Bucket(self.bucket).download_fileobj(latest_model, data)
            data.seek(0)
            model = pickle.load(data)
        
        predictor = PredictionsCreator(model, self.year, self.month, self.day, self.matrix_uuid)
        self._rows, self.columns = predictor.create_predictions()

        super().run()
        
    def rows(self):
        for element in self._rows:
            yield element

    def get_lastest_model(self, session):
        s3_client = session.client('s3')
        response = s3_client.list_objects_v2(Bucket=self.bucket)
        all_models = response['Contents']
        latest = max(all_models, key=lambda x: x['LastModified'])
        return latest['Key']