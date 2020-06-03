import pandas as pd
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class DataPreparator:
    def __init__(self):
        host, database, user, password = get_database_connection_parameters()
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)

    def split_train_test(self):
        tabla_3 = pd.read_sql_table('centers', self.engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', self.engine, schema="transformed")

        tabla_3 = tabla_3.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent', 'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)

        tabla_3.rename(columns={'dc_id':'center_id'}, inplace=True)
        tabla_3 = tabla_3.reset_index(drop=True)
        tabla_5 = pd.merge(tabla_4, tabla_3)
        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)

        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype(float)

        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_nan'], axis=1)

        tabla_5.center_id = tabla_5.center_id.apply(lambda x: x.replace('dc', ''))
        tabla_5.center_id  = tabla_5.center_id.astype(int)
        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5.select_dtypes(object):
            tabla_5[col] = tabla_5[col].astype(float)

        tabla_5 = tabla_5.fillna(0)

        df_train = tabla_5.loc[(tabla_5['inspection_year'] <= 2019.0) & (tabla_5['inspection_month'] <= 11.0)]
        df_test = tabla_5.loc[(tabla_5['inspection_year'] == 2019.0) & (tabla_5['inspection_month'] == 12.0)]

        Y_train = df_train[['violationcategory_public_health_hazard']]
        Y_test = df_test[['violationcategory_public_health_hazard']]
        X_train = df_train[[i for i in df_train.keys() if i not in Y_train]]
        X_test = df_test[[i for i in df_test.keys() if i not in Y_test]]
        X_train = X_train.fillna(0)
        X_test = X_test.fillna(0)
        Y_train = Y_train.fillna(0)
        Y_test = Y_test.fillna(0)
        return X_train, Y_train, X_test, Y_test

    def convert_to_arrarys(self):
        tabla_3 = pd.read_sql_table('centers', self.engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', self.engine, schema="transformed")

        tabla_3 = tabla_3.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent', 'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)

        tabla_3.rename(columns={'dc_id':'center_id'}, inplace=True)
        tabla_3 = tabla_3.reset_index(drop=True)
        tabla_5 = pd.merge(tabla_4, tabla_3)
        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)

        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype(float)

        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_nan'], axis=1)

        tabla_5.center_id = tabla_5.center_id.apply(lambda x: x.replace('dc', ''))
        tabla_5.center_id  = tabla_5.center_id.astype(int)
        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5.select_dtypes(object):
            tabla_5[col] = tabla_5[col].astype(float)

        tabla_5 = tabla_5.fillna(0)
    
        X = tabla_5.loc[:, tabla_5.columns != 'violationcategory_public_health_hazard'].values
        y = tabla_5[['violationcategory_public_health_hazard']].values
        return X, y

    def create_table(self):
        tabla_3 = pd.read_sql_table('centers', self.engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', self.engine, schema="transformed")

        tabla_3 = tabla_3.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent', 'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)

        tabla_3.rename(columns={'dc_id':'center_id'}, inplace=True)
        tabla_3 = tabla_3.reset_index(drop=True)
        tabla_5 = pd.merge(tabla_4, tabla_3)
        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)

        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype(float)

        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_nan'], axis=1)

        tabla_5.center_id = tabla_5.center_id.apply(lambda x: x.replace('dc', ''))
        tabla_5.center_id  = tabla_5.center_id.astype(int)
        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5.select_dtypes(object):
            tabla_5[col] = tabla_5[col].astype(float)

        tabla_5 = tabla_5.fillna(0)

        return tabla_5

    