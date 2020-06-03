import luigi
import luigi.contrib.s3

from nyc_ccci_etl.model.nyc_ccci_random_forest import NYCCCCIRandomForest
import pickle
from nyc_ccci_etl.orchestrator_tasks.model_metadata import ModelMetadata
from nyc_ccci_etl.commons.configuration import get_aws_bucket
class FitRandomForestAndCreatePickle(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    

    def requires(self):
        return ModelMetadata(self.year, self.month, self.day) 

    def output(self):
        bucket = get_aws_bucket()
        output_path = "s3://{}/random_forest_{}_{}_{}.pckl".\
        format(
            str(bucket),
            str(self.year),
            str(self.month),
            str(self.day),
        )
        return luigi.contrib.s3.S3Target(path=output_path, format=luigi.format.Nop)

    def run(self):
        random_forest = NYCCCCIRandomForest(self.year, self.month, self.day)
        result = random_forest.fit()

        with self.output().open('w') as output_pickle:
            pickle.dump(result, output_pickle)