import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class CentersUpdater:
    def __init__(self, year, month, day):
        host, database, user, password = get_database_connection_parameters()
        self.date_filter = "{}_{}_{}t00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)

    def execute(self):
        df = pd.read_sql("select * from clean.inspections where inspectiondate='{}'".format(self.date_filter), self.engine)
        #Seleccionando las variables estaticas de la tabla limpia
        tabla_3 = df.loc[:, ['centername', 'legalname', 'building', 'street', 'borough', 'zipcode', 'phone', 'permitnumber', 
                     'permitexp', 'status', 'agerange', 'maximumcapacity', 'dc_id', 'programtype', 'facilitytype', 
                     'childcaretype', 'bin', 'url', 'datepermitted', 'actual', 'violationratepercent', 'violationavgratepercent', 
                     'totaleducationalworkers', 'averagetotaleducationalworkers', 'publichealthhazardviolationrate', 
                     'averagepublichealthhazardiolationrate', 'criticalviolationrate', 'avgcriticalviolationrate']]

        #Tirando los duplicados para que queden los centros únicos y sus características
        tabla_3 = tabla_3.drop_duplicates()

        #Preprando para one_hot_encoding
        categorias = ["programtype", "facilitytype", "borough"]
        df_1 = pd.get_dummies(tabla_3[categorias])
        tabla_3 = tabla_3.join(df_1)

        tabla_3 = tabla_3.drop(['programtype', 'facilitytype', 'borough'], axis = 1)

        old = pd.read_sql('select dc_id from transformed.centers', self.engine)
        
        tabla_3 = tabla_3[~tabla_3['dc_id'].isin(old.dc_id)]
        tabla_3 = tabla_3.drop_duplicates()
        

        return [tuple(x) for x in tabla_3.to_numpy()], [(c, 'VARCHAR') for c in list(tabla_3.columns)]
