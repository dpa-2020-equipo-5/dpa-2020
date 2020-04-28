#Autor: Gerardo Mathus - @gemathus

import configparser
import pandas as pd
import clean_data
from eda_gda.data_profile import DataProfile

config = configparser.ConfigParser()
config.read_file(open('./settings.ini'))

raw_dataset_path = config.get('DATASET','raw_path')
clean_dataset_path = config.get('DATASET','clean_path')

print("1/4 Correr script clean_data.py")
clean_data.execute(raw_dataset_path, clean_dataset_path)

print("2/4 Leer dataset limpio a un dataframe")
df = pd.read_csv(clean_dataset_path)

print("3/4 Crear data profiling inicial automatizado")

#numerics_to_ignore = ['gid','anio', 'latitud', 'longitud']
date_columns = ['inspection_date']

#variables numéricas que queremos forzar a que se traten como categóricas
force_cats = ['zipcode']

print("4/4 Data Profiling")
#data_profile = DataProfile('ILE', df, date_variables=date_columns, force_categorics=force_cats)
data_profile = DataProfile('ChildcareCenters', df, data_variables=date_columns, force_categorics=force_cats)
md_file, md_content = data_profile.get_profile_as_md()

print("+"*50)
print("FIN")


