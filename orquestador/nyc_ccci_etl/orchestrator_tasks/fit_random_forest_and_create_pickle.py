import luigi
import luigi.contrib.s3

from nyc_ccci_etl.model.nyc_ccci_random_forest import NYCCCCIRandomForest
import pickle
from nyc_ccci_etl.orchestrator_tasks.model_metadata import ModelMetadata
class FitRandomForestAndCreatePickle(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    

    def requires(self):
        return ModelMetadata(self.year, self.month, self.day) 

    def output(self):
        output_path = "s3://nyc-ccci/random_forest_{}_{}_{}.pckl".\
        format(
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