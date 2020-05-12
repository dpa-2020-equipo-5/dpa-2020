import numpy as np
import pandas as pd
from sodapy import Socrata

#Leyendo los datos
MyAppToken = ''

client = Socrata("data.cityofnewyork.us", MyAppToken)

results = client.get("dsg6-ifza", limit = 60000)

df = pd.DataFrame.from_records(results)
                               
#Definiendo funciones

def clean_string(astr):
    '''
    Esta función reemplaza caracteres por espacios, espacios por guiones bajos, y el caracter ñ por 'ni'. 
    También quita acentos. 
    '''
    return astr.lower().replace('.', '') \
        .replace(',', '') \
        .replace(';', '') \
        .replace(':', '') \
        .replace('á', 'a') \
        .replace('é', 'e') \
        .replace('í', 'i') \
        .replace('ó', 'o') \
        .replace('ú', 'u') \
        .replace(' ', '_') \
        .replace('ñ', 'ni')

def clean_columns(df):
    '''
    Esta función corre la función clean_string pero para los nombres de las columnas. 
    '''
    for series in df:
        df.rename(columns={series:clean_string(series)}, inplace=True)
    
#Limpiando la base

print("\t-> Limpando los nombres de las columnas")
clean_columns(df)

print("\t-> Reemplazando espacios en blanco")
for col in df.select_dtypes('object'):
    df[col] = df[col].replace('\s+', ' ', regex=True)
    
print("\t-> Limpiando las observaciones")
for col in df.select_dtypes('object'):
        df[col] = df[col].str.strip()
        df[col] = df[col].str.lower()
        df[col] = df[col].str.replace('á', 'a')
        df[col] = df[col].str.replace('é', 'e')
        df[col] = df[col].str.replace('í', 'i')
        df[col] = df[col].str.replace('ó', 'o')
        df[col] = df[col].str.replace('ú', 'u')
        df[col] = df[col].str.replace(' ', '_')

print("\t-> Cambiando NA por np.nan")
for col in df.select_dtypes('object'):
    df.loc[df[col] == 'na', col] = np.nan

print("\t-> Eliminando duplicados")
df = df.drop_duplicates()
df.shape
