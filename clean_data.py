import pandas as pd
import numpy as np
from eda_gda.commons import clean_columns, clean_string

def execute(raw_dataset_path, clean_dataset_path):
    print("\t-> Leyendo datos crudos a un DataFrame")
    df = pd.read_csv(raw_dataset_path)

    #limpiar columnbas
    print("\t-> Limpando columnas")
    clean_columns(df)

    print("\t-> Reemplazando espacios en blanco")
    for col in df.select_dtypes('object'):
        df[col] = df[col].replace('\s+', ' ', regex=True)

    print("\t-> Limpiando valores")
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

    df.to_csv(clean_dataset_path, index=False)