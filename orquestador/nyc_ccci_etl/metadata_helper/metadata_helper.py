import pandas as pd
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class MetadataHelper:
    def __init__(self, year, month, day):
        host, database, user, password = get_database_connection_parameters()
        self.date_filter = "{}-{}-{}T00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        self.date_filter_clean = "{}_{}_{}t00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        self.date_filter_transformed = "{}-{}-{} 00:00:00".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        self.date_filter_aequitas = "{}-{}-{}".format(str(year), str(month).zfill(2), str(day).zfill(2))
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)

    def get_inserted_raw_records(self):
        df = pd.read_sql("select count(*) from raw.inspections where inspection->>'inspectiondate' = '{}';".format(self.date_filter), self.engine)
        return df['count'].values[0]
        
    def get_inserted_raw_columns(self):
        df = pd.read_sql("select * from raw.inspections where inspection->>'inspectiondate' = '{}' limit 1;".format(self.date_filter), self.engine)
        return ",".join(df['inspection'].values[0].keys())

    def get_inserted_clean_records(self):
        df = pd.read_sql("select count(*) from clean.inspections where inspectiondate= '{}';".format(self.date_filter_clean), self.engine)
        return df['count'].values[0]
        
    def get_inserted_clean_columns(self):
        df = pd.read_sql("select * from clean.inspections where inspectiondate= '{}' limit 1;".format(self.date_filter_clean), self.engine)
        return ",".join(df.columns)
    
    def get_inserted_transformed_records(self):
        df = pd.read_sql("select count(*) from transformed.inspections where inspectiondate= '{}';".format(self.date_filter_transformed), self.engine)
        return df['count'].values[0]
        
    def get_inserted_transformed_columns(self):
        df = pd.read_sql("select * from transformed.inspections where inspectiondate= '{}' limit 1;".format(self.date_filter_transformed), self.engine)
        return ",".join(df.columns)

    def get_inserted_predictions(self):
        df = pd.read_sql("select count(*) from predictions.predictions where date= '{}';".format(self.date_filter_transformed), self.engine)
        return df['count'].values[0]

    def get_inserted_aequitas_groups(self):
        df = pd.read_sql("select count(*) from aequitas.groups where date= '{}';".format(self.date_filter_aequitas), self.engine)
        return df['count'].values[0]

    def get_inserted_aequitas_fairness(self):
        df = pd.read_sql("select count(*) from aequitas.fairness where date= '{}';".format(self.date_filter_aequitas), self.engine)
        return df['count'].values[0]

    def get_inserted_aequitas_bias(self):
        df = pd.read_sql("select count(*) from aequitas.bias where date= '{}';".format(self.date_filter_aequitas), self.engine)
        return df['count'].values[0]