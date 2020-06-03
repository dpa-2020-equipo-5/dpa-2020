import json
import luigi
from datetime import datetime
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.utils.get_os_user import get_os_user
from nyc_ccci_etl.utils.get_current_ip import get_current_ip

from nyc_ccci_etl.orchestrator_tasks.load_aequitas_groups import LoadAequitasGroups
from nyc_ccci_etl.orchestrator_tasks.load_aequitas_bias import LoadAequitasBias
from nyc_ccci_etl.orchestrator_tasks.load_aequitas_fairness import LoadAequitasFairness

from nyc_ccci_etl.metadata_helper.metadata_helper import MetadataHelper

class LoadBiasFairnessMetadata(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    pipeline_type = luigi.Parameter()
    
    def requires(self):
        return (
            LoadAequitasGroups(self.year, self.month, self.day, self.pipeline_type),
            LoadAequitasBias(self.year, self.month, self.day, self.pipeline_type),
            LoadAequitasFairness(self.year, self.month, self.day, self.pipeline_type)
        )

    host, database, user, password = get_database_connection_parameters()
    table = "aequitas.metadata"
    schema = "aequitas"
    columns = [ 
        ("executed_at", "timestamp"),
        ("task_params", "varchar"),
        ("bias_records", "integer"),
        ("fairness_records", "integer"),
        ("groups_records", "integer"),
        ("execution_user", "varchar"),
        ("source_ip", "varchar"),
    ]
    def run(self):
        helper = MetadataHelper(self.year, self.month, self.day)
        self.inserted_bias_records = helper.get_inserted_aequitas_bias()
        self.inserted_fairness_records = helper.get_inserted_aequitas_fairness()
        self.inserted_groups_records = helper.get_inserted_aequitas_groups()
        super().run()
    

    def rows(self):
        params_string = "year={} month={} day={}".format(str(self.year), str(self.month), str(self.day))
        row = (
            str(datetime.now(tz=None)),
            params_string,
            self.inserted_bias_records,
            self.inserted_fairness_records,
            self.inserted_groups_records,
            get_os_user(),
            get_current_ip()
        )
        yield row