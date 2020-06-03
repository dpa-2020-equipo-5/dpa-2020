import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
import json
from datetime import datetime
class InspectionsTransformer:
    def __init__(self, year, month, day):
        self.date_filter = "{}_{}_{}t00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        host, database, user, password = get_database_connection_parameters()
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
        
        tabla_4 = df.loc[:, ['dc_id', 'inspectiondate', 'regulationsummary', 'violationcategory', 'healthcodesubsection', 'violationstatus', 'inspectionsummaryresult', 'borough']]
        tabla_4['inspectionsummaryresult'] = tabla_4['inspectionsummaryresult'].astype('str')

        #Cambia de .split('_-_',1) a split('___',1) porque clean.py reemplaza - por _
        df_2 = pd.DataFrame(tabla_4.inspectionsummaryresult.str.split('___',1).tolist(), columns= ['reason', 'result'])
        
        df_2['result'] = df_2['result'].astype('str')
        df_3 = pd.DataFrame(df_2.result.str.split(';_',1).tolist(), columns = ['result_1', 'result_2'])

        df_2 = df_2.drop(df_2.columns[[1]], axis=1) 
        df_2 = df_2.join(df_3)
        tabla_4 = tabla_4.join(df_2)
        tabla_4 = tabla_4.drop(['inspectionsummaryresult'], axis = 1) #Eliminar inspection_summary_result
        tabla_4.reason.value_counts(dropna=False)
        tabla_4 = tabla_4.loc[tabla_4['reason'] == 'initial_annual_inspection']
        tabla_4['result_2'] = tabla_4['result_2'].fillna('NR')
        
        categorias = ["result_1", "result_2"]
        df_4 = pd.get_dummies(tabla_4[categorias])
        tabla_4 = tabla_4.join(df_4)
        tabla_4 = tabla_4.drop(['result_1', 'result_2'], axis = 1) #Eliminamos variables que no necesitamos
        tabla_4['inspectiondate'] = tabla_4['inspectiondate'].astype('str')
        tabla_4['inspectiondate'] = pd.to_datetime(tabla_4.inspectiondate, infer_datetime_format=False, format='%Y_%m_%dt00:00:00.000')
        
        tabla_4['inspection_year'] = tabla_4['inspectiondate'].dt.year
        tabla_4['inspection_month'] = tabla_4['inspectiondate'].dt.month
        tabla_4['inspection_day_name'] = tabla_4['inspectiondate'].dt.day_name()
        
        tabla_4 = tabla_4.drop(tabla_4.loc[tabla_4['inspection_day_name']== 'Saturday'].index)
        tabla_4 = tabla_4.drop(tabla_4.loc[tabla_4['inspection_day_name']== 'Sunday'].index)

        dias = {"Monday":'1', "Tuesday":'2', "Wednesday":'3', "Thursday":'4',"Friday":'5'}
        
        tabla_4['inspection_day_name'] = tabla_4['inspection_day_name'].map(dias)
        tabla_4['inspection_day_name'] = tabla_4['inspection_day_name'].astype(float)
        
        tabla_4.rename(columns={'dc_id':'center_id'}, inplace=True)
        tabla_4.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        categorias = ["violationcategory"]
        df_5 = pd.get_dummies(tabla_4[categorias])

        encoded_columns = ["violationcategory_public_health_hazard", "violationcategory_critical", "violationcategory_general"]
        for col in encoded_columns:
            if col not in df_5.columns:
                df_5[col] = 0

        tabla_4 = tabla_4.join(df_5)
        tabla_4 = tabla_4.drop(['violationcategory'], axis = 1) #Eliminamos variables que no necesitamos
        df_6 = tabla_4.loc[tabla_4['inspection_year']!=2020.0]
        df_7 = pd.DataFrame(df_6.groupby(["center_id"], sort=False)["inspectiondate"].max().reset_index())
        
        year = str(datetime.now().year)
        month = str(datetime.now().month)
        day = str(datetime.now().day)
        
        fechas = year + "-" + month + "-" + day
        
        df_7["today"] = pd.to_datetime(fechas)
        df_7['dias_ultima_inspeccion'] = df_7['today'] - df_7['inspectiondate']
        df_7['dias_ultima_inspeccion'] = df_7['dias_ultima_inspeccion'].dt.days
        tabla_4 = pd.merge(tabla_4, df_7, left_on='center_id', right_on='center_id', how='left')
        tabla_4 =  tabla_4.rename(columns = {'inspectiondate_x':'inspectiondate'})
        tabla_4 = tabla_4.drop(['today', 'inspectiondate_y'], axis = 1)
        
        df_8 = pd.DataFrame(df_6.groupby(["center_id"], sort=False)["violationcategory_public_health_hazard"].sum().reset_index())
        df_8 =  df_8.rename(columns = {'violationcategory_public_health_hazard':'violaciones_hist_salud_publica'})
        tabla_4 = pd.merge(tabla_4, df_8, left_on='center_id', right_on='center_id', how='left')
        
        df_9 = tabla_4.loc[tabla_4['inspection_year']==2019.0]
        
        df_10 = pd.DataFrame(df_9.groupby(["center_id"], sort=False)["violationcategory_public_health_hazard"].sum().reset_index())
        df_10 =  df_10.rename(columns = {'violationcategory_public_health_hazard':'violaciones_2019_salud_publica'})
        tabla_4 = pd.merge(tabla_4, df_10, left_on='center_id', right_on='center_id', how='left')
        
        df_11 = pd.DataFrame(df_6.groupby(["center_id"], sort=False)["violationcategory_critical"].sum().reset_index())
        df_11 =  df_11.rename(columns = {'violationcategory_critical':'violaciones_hist_criticas'})
        
        tabla_4 = pd.merge(tabla_4, df_11, left_on='center_id', right_on='center_id', how='left')
        df_12 = pd.DataFrame(df_9.groupby(["center_id"], sort=False)["violationcategory_critical"].sum().reset_index())
        df_12 =  df_12.rename(columns = {'violationcategory_critical':'violaciones_2019_criticas'})
        tabla_4 = pd.merge(tabla_4, df_12, left_on='center_id', right_on='center_id', how='left')
        df_13 = pd.merge(df_8, df_11)
        df_13['total'] = df_13['violaciones_hist_salud_publica'] + df_13['violaciones_hist_criticas']
        df_14 = pd.DataFrame(df_6.groupby(["center_id"], sort=False)["reason"].count().reset_index())
        df_15 = pd.merge(df_13, df_14)
        df_15['ratio_violaciones_hist'] = df_15['total'] / df_15['reason']
        df_15 = df_15.drop(['violaciones_hist_salud_publica', 'violaciones_hist_criticas', 'total', 'reason'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_15, left_on='center_id', right_on='center_id', how='left')
        df_16 = pd.merge(df_10, df_12)
        df_16['total'] = df_16['violaciones_2019_salud_publica'] + df_16['violaciones_2019_criticas']
        df_17 = pd.DataFrame(df_9.groupby(["center_id"], sort=False)["reason"].count().reset_index())
        df_18 = pd.merge(df_16, df_17)
        df_18['ratio_violaciones_2019'] = df_18['total'] / df_18['reason']
        df_18 = df_18.drop(['violaciones_2019_salud_publica', 'violaciones_2019_criticas', 'total', 'reason'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_18, left_on='center_id', right_on='center_id', how='left')
        df_19 = pd.DataFrame(df_6.groupby(["borough"], sort=False)[["violationcategory_critical", "violationcategory_general", "violationcategory_public_health_hazard"]].sum().reset_index())
        df_19['prom_violaciones_hist_borough'] = df_19[['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard']].mean(axis=1)
        df_19 = df_19.drop(['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_19, left_on='borough', right_on='borough', how='left')
        df_20 = pd.DataFrame(df_9.groupby(["borough"], sort=False)[["violationcategory_critical", "violationcategory_general", "violationcategory_public_health_hazard"]].sum().reset_index())
        df_20['prom_violaciones_2019_borough'] = df_20[['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard']].mean(axis=1)
        df_20 = df_20.drop(['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_20, left_on='borough', right_on='borough', how='left')
        df_21 = pd.DataFrame(df_6.groupby(["center_id"], sort=False)[["violationcategory_critical", "violationcategory_general", "violationcategory_public_health_hazard"]].sum().reset_index())
        df_21['total'] = df_21['violationcategory_critical'] + df_21['violationcategory_general'] + df_21['violationcategory_public_health_hazard']
        df_21['ratio_violaciones_hist_sp'] = df_21['violationcategory_public_health_hazard'] / df_21['total']
        df_21 = df_21.drop(['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard', 'total'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_21, left_on='center_id', right_on='center_id', how='left')
        df_22 = pd.DataFrame(df_9.groupby(["center_id"], sort=False)[["violationcategory_critical", "violationcategory_general", "violationcategory_public_health_hazard"]].sum().reset_index())
        df_22['total'] = df_22['violationcategory_critical'] + df_22['violationcategory_general'] + df_22['violationcategory_public_health_hazard']
        df_22['ratio_violaciones_2019_sp'] = df_22['violationcategory_public_health_hazard'] / df_22['total']
        df_22 = df_22.drop(['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard', 'total'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_22, left_on='center_id', right_on='center_id', how='left')
        df_23 = pd.DataFrame(df_6.groupby(["center_id"], sort=False)[["violationcategory_critical", "violationcategory_general", "violationcategory_public_health_hazard"]].sum().reset_index())
        df_23['total'] = df_23['violationcategory_critical'] + df_23['violationcategory_general'] + df_23['violationcategory_public_health_hazard']
        df_23['ratio_violaciones_hist_criticas'] = df_23['violationcategory_critical'] / df_23['total']
        df_23 = df_23.drop(['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard', 'total'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_23, left_on='center_id', right_on='center_id', how='left')
        df_24 = pd.DataFrame(df_9.groupby(["center_id"], sort=False)[["violationcategory_critical", "violationcategory_general", "violationcategory_public_health_hazard"]].sum().reset_index())
        df_24['total'] = df_24['violationcategory_critical'] + df_24['violationcategory_general'] + df_24['violationcategory_public_health_hazard']
        df_24['ratio_violaciones_2019_criticas'] = df_24['violationcategory_critical'] / df_24['total']
        df_24 = df_24.drop(['violationcategory_critical', 'violationcategory_general', 'violationcategory_public_health_hazard', 'total'], axis = 1)
        tabla_4 = pd.merge(tabla_4, df_24, left_on='center_id', right_on='center_id', how='left')
        self.output_table = tabla_4.copy()
        return [tuple(x) for x in tabla_4.to_numpy()], [(c, 'VARCHAR') for c in list(tabla_4.columns)]  