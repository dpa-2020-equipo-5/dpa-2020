

import json
import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.orchestrator_tasks.fit_random_forest_and_create_pickle import FitRandomForestAndCreatePickle

from nyc_ccci_etl.ccci_aequitas.bias_metrics import BiasMetrics

class LoadAequitasBias(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    pipeline_type = luigi.Parameter()
    def requires(self):
        if (str(self.pipeline_type) == 'train'):
            return FitRandomForestAndCreatePickle(self.year, self.month, self.day)
        
    host, database, user, password = get_database_connection_parameters()
    table = "aequitas.bias"
    schema = "aequitas"
    def run(self):
        b = BiasMetrics(self.year, self.month, self.day)
        self._rows, self.columns = b.execeute()
        super().run()

    def rows(self):
        for element in self._rows:
            yield element