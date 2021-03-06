# -*- coding: utf-8 -*-
"""
Created on Tue May 12 07:30:23 2020

@author: Elizabeth
"""

print("\t-> Conservar únicamente las variables estáticas que se utilizarán en el modelo (tabla_3): dc_id, maximumcapacity, totaleducationalworkers, averagetotaleducationalworkers y las 13 variables creadas que corresponden a la categorización de las variables borough, program_type, facility_type")

tabla_3 = tabla_3.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  
                        'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent',
                        'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)

tabla_3.rename(columns={'dc_id':'center_id'}, inplace=True)

tabla_3 = tabla_3.reset_index(drop=True)

tabla_5 = pd.merge(tabla_4, tabla_3)

tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)

tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype('int')

tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype('int')

tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype('int')

tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype('float')

tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_NP'], axis=1)

tabla_5 = tabla_5.set_index(['center_id'])

tabla_5 = tabla_5.fillna(0)

print("\t-> Para el entrenamiento se usaron todos los datos del 2017-2019 y para validación los datos correspondientes a lo que va del año 2020.")

df_train = tabla_5.loc[tabla_5['inspection_year'] != 2020.0]

df_test = tabla_5.loc[tabla_5['inspection_year'] == 2020.0]

Y_train = df_train[['violationcategory_public_health_hazard']]

Y_test = df_test[['violationcategory_public_health_hazard']]

X_train = df_train[[i for i in df_train.keys() if i not in Y_train]]

X_test = df_test[[i for i in df_test.keys() if i not in Y_test]]
