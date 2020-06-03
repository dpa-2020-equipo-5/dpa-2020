import luigi
import sys
from luigi.contrib.postgres import CopyToTable
import boto3
import pickle
from io import BytesIO
from nyc_ccci_etl.tests.test_predictions import TestPredictions
from nyc_ccci_etl.utils.print_with_format import print_test_failed, print_test_passed
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class AtLeastOneCenterValidation(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    matrix_uuid = luigi.Parameter()

    host, database, user, password = get_database_connection_parameters()
    table = "testing.predictions"
    schema = "testing"

    columns = [
        ('test', 'varchar'),
        ('ran_at', 'timestamp'),
        ('params', 'varchar'),
        ('status', 'varchar'),
        ('note', 'varchar')
    ]
    def get_lastest_model(self, session):
        s3_client = session.client('s3')
        response = s3_client.list_objects_v2(Bucket='nyc-ccci')
        all_models = response['Contents']
        latest = max(all_models, key=lambda x: x['LastModified'])
        return latest['Key']

    def run(self):
        ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        latest_model = self.get_lastest_model(ses)
        s3_resource = ses.resource('s3')
        with BytesIO() as data:
            s3_resource.Bucket("nyc-ccci").download_fileobj(latest_model, data)
            data.seek(0)
            model = pickle.load(data)


        test_preds = TestPredictions()
        self.test_result = test_preds.test_at_least_one_center(model, self.year, self.month, self.day, self.matrix_uuid)
        if self.test_result['status'] == 'failed':
            print_test_failed(self.test_result['test'], self.test_result['note'])
            sys.exit()
        else:
            print_test_passed(self.test_result['test'])
            
        super().run()
    
    def rows(self):
        params = "year={} month={} day={}".format(self.year, self.month, self.day)
        yield (self.test_result['test'], self.test_result['ran_at'], params, self.test_result['status'], self.test_result['note'])
        