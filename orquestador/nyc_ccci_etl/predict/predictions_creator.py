import pandas as pd
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from datetime import datetime
class PredictionsCreator:
    def __init__(self, model, year, month, day, uuid):
        host, database, user, password = get_database_connection_parameters()
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)
        self.model = model
        self.year = year
        self.month = month
        self.day = day
        self.matrix_uuid = uuid
    def create_predictions(self):
        centros = pd.read_sql_table('centers', self.engine, schema="transformed")
        centros.rename(columns={"dc_id":"center_id"}, inplace=True)
        inspecciones = pd.read_sql_table('inspections', self.engine, schema="transformed")
        last_inspections = inspecciones.sort_values(by="inspectiondate").drop_duplicates(subset=["center_id"], keep="last")
        centros = centros.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent', 'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)
        centros = centros.reset_index(drop=True)
        tabla_5 = pd.merge(last_inspections, centros)
        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype(float)

        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_nan'], axis=1)

        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5.select_dtypes(object):
            tabla_5[col] = tabla_5[col].astype(float)

        tabla_5 = tabla_5.fillna(0)

        prds = self.model.predict(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))
        probas = self.model.predict_proba(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))

        res = pd.DataFrame({
            "center":tabla_5.index,
            "etiqueta":prds,
            "proba_0":probas[:,0],
            "proba_1":probas[:,1]
        })
        predicciones = res[res.etiqueta == 1].copy()
        predicciones.sort_values(by=['proba_1'], ascending=False, inplace=True)
        predicciones['date'] = "{}-{}-{} 00:00:00".format(str(self.year).zfill(2), str(self.month).zfill(2), str(self.day).zfill(2))
        predicciones.drop(['proba_0'], axis=1,inplace=True)
        predicciones["priority"] = range(1, len(predicciones) + 1)
        predicciones.columns = ['center_id', 'class', 'probability', 'date', 'priority']
        predicciones.drop(['class'], axis=1, inplace=True)
        predicciones.probability = predicciones.probability.astype(float)
        predicciones.priority = predicciones.priority.astype(int)
        predicciones['matrix_uuid'] = self.matrix_uuid
        self.output_table = predicciones
        return [tuple(x) for x in predicciones.to_numpy()], [
            ('center_id', 'VARCHAR'),('probability', 'FLOAT'),('date', 'timestamp'),('priority', 'INTEGER'), ('matrix_uuid', 'VARCHAR')
        ]  