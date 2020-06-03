import json
import luigi
from luigi.contrib.postgres import CopyToTable
from datetime import datetime

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.utils.get_os_user import get_os_user
from nyc_ccci_etl.utils.get_current_ip import get_current_ip

from .load_transformed_inspections import LoadTransformedInspections
from nyc_ccci_etl.metadata_helper.metadata_helper import MetadataHelper
class LoadTransformedInspectionsMetadata(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    def requires(self):
        return  LoadTransformedInspections(self.year, self.month, self.day)
    
    
    host, database, user, password = get_database_connection_parameters()
    table = "transformed.metadata"
    schema = "transformed"
    columns = [ 
        ("executed_at", "timestamp"),
        ("task_params", "varchar"),
        ("record_count", "integer"),
        ("execution_user", "varchar"),
        ("source_ip", "varchar"),
        ("database_name", "varchar"),
        ("database_schema", "varchar"),
        ("database_table", "varchar"),
        ("database_user", "varchar"),
        ("vars", "varchar"),
        ("script_tag", "varchar")
    ]
    def run(self):
        helper = MetadataHelper(self.year, self.month, self.day)
        self.inserted_columns = helper.get_inserted_transformed_columns()
        self.inserted_record_count = helper.get_inserted_transformed_records()
        super().run()
    

    def rows(self):
        params_string = "year={} month={} day={}".format(str(self.year), str(self.month), str(self.day))
        row = (
            str(datetime.now(tz=None)),
            params_string,
            self.inserted_record_count,
            get_os_user(),
            get_current_ip(),
            self.database,
            self.schema,
            self.table,
            self.user,
            self.inserted_columns,
            "transormations"
        )
        yield row