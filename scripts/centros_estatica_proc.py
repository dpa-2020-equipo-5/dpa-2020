import numpy as np
import pandas as pd
import os

#Seleccionando las variables estaticas de la tabla limpia
tabla_3 = df.iloc[:, 0:28]

#Tirando los duplicados para que queden los centros únicos y sus características
tabla_3 = tabla_3.drop_duplicates()
tabla_3.shape

#Creando dummies para borough, program_type, facility_type
dummies = ["borough","program_type", "facility_type"]
df_1 = pd.get_dummies(tabla_3[dummies])
tabla_3 = tabla_3.join(df_1)

#Seleccionando las varibales a usar en el modelo
tabla_3 = tabla_3.drop(df.columns[[0,1,2,3,5,6,7,8,9,10,15,16,17,18,19,21,23,25,27]], axis=1)