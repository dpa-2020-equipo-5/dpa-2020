import json
import luigi
import uuid
from luigi.contrib.postgres import CopyToTable
from datetime import datetime

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.utils.get_os_user import get_os_user
from nyc_ccci_etl.utils.get_current_ip import get_current_ip

from .create_predictions import CreatePredictions
from nyc_ccci_etl.metadata_helper.metadata_helper import MetadataHelper
class LoadPredictionsMetadata(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    matrix_uuid =  str(uuid.uuid4())
    pipeline_type=luigi.Parameter()
    def requires(self):
        return  CreatePredictions(self.year, self.month, self.day, self.matrix_uuid, self.pipeline_type)
    
    
    host, database, user, password = get_database_connection_parameters()
    table = "predictions.metadata"
    schema = "predictions"
    columns = [ 
        ("executed_at", "timestamp"),
        ("matrix_uuid", "varchar"),
        ("task_params", "varchar"),
        ("total_predictions", "integer"),
        ("execution_user", "varchar"),
        ("source_ip", "varchar"),
        ("script_tag", "varchar")
    ]
    def run(self):
        helper = MetadataHelper(self.year, self.month, self.day)
        self.inserted_record_count = helper.get_inserted_predictions()
        super().run()
    

    def rows(self):
        params_string = "year={} month={} day={}".format(str(self.year), str(self.month), str(self.day))
        row = (
            str(datetime.now(tz=None)),
            self.matrix_uuid,
            params_string,
            self.inserted_record_count,
            get_os_user(),
            get_current_ip(),
            "https://github.com/dpa-2020-equipo-5/nyc-ccci-etl/blob/master/nyc_ccci_etl/predict/predictions_creator.py"
        )
        yield row