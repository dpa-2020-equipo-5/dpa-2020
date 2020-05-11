import numpy as np
import pandas as pd
import os

tabla_3 = df.loc[:, ['centername', 'legalname', 'building', 'street', 'borough', 'zipcode', 'phone', 'permitnumber', 
                     'permitexp', 'status', 'agerange', 'maximumcapacity', 'dc_id', 'programtype', 'facilitytype', 
                     'childcaretype', 'bin', 'url', 'datepermitted', 'actual', 'violationratepercent', 'violationavgratepercent', 
                     'totaleducationalworkers', 'averagetotaleducationalworkers', 'publichealthhazardviolationrate', 
                     'averagepublichealthhazardiolationrate', 'criticalviolationrate', 'avgcriticalviolationrate']]

tabla_3.info()

tabla_3 = tabla_3.drop_duplicates()

tabla_3.shape

categorias = ["programtype", "facilitytype", "borough"]

df_1 = pd.get_dummies(tabla_3[categorias])

tabla_3 = tabla_3.join(df_1)

tabla_3 = tabla_3.drop(['programtype', 'facilitytype', 'borough'], axis = 1)

tabla_3.info()
