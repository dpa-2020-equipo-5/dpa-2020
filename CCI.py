#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns #Control figure 
import numpy as np
import os
matplotlib.style.use('ggplot')
#get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


from sodapy import Socrata


# In[3]:


#MyAppToken = ''


# In[4]:


#client = Socrata("data.cityofnewyork.us", MyAppToken)


# In[5]:


#results = client.get("dsg6-ifza", limit=2000)


# In[6]:


#df = pd.DataFrame.from_records(results)


# In[7]:


cwd = os.getcwd() #to get current working directory
#print(cwd)


# In[8]:


df = pd.read_csv('DOHMH_Childcare_Center_Inspections.csv', encoding = "L1")


# In[9]:


df.info()


# In[10]:


def clean_string(astr):
    return astr.lower().replace('.', '')         .replace(',', '')         .replace(';', '')         .replace(':', '')         .replace('á', 'a')         .replace('é', 'e')         .replace('í', 'i')         .replace('ó', 'o')         .replace('ú', 'u')         .replace(' ', '_')         .replace('ñ', 'ni')


# In[11]:


def clean_columns(df):
    for series in df:
        df.rename(columns={series:clean_string(series)}, inplace=True)


# In[12]:


def execute(raw_dataset_path, clean_dataset_path):
    print("\t-> Leyendo datos crudos a un DataFrame")
    df = pd.read_csv('DOHMH_Childcare_Center_Inspections.csv')


# In[13]:


print("\t-> Limpando columnas")


# In[14]:


clean_columns(df)


# In[15]:


df.info()


# In[16]:


print("\t-> Reemplazando espacios en blanco")
for col in df.select_dtypes('object'):
    df[col] = df[col].replace('\s+', ' ', regex=True)


# In[17]:


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


# In[18]:


print("\t-> Cambiando NA por np.nan")
for col in df.select_dtypes('object'):
    df.loc[df[col] == 'na', col] = np.nan


# In[19]:


df.to_csv('df.csv', index=False)


# In[20]:


print("\t-> Cuántos valores NaN tiene la base")


# In[21]:


df.isnull().sum()


# In[22]:


print("\t-> Eliminar duplicados")


# In[23]:


df.duplicated().sum()


# In[24]:


df = df.drop_duplicates()
df.shape


# ### TABLA 3

# In[25]:


dummies = ["borough", "status", "program_type", "facility_type"]
df_1 = df[[i for i in df.keys() if i not in dummies]]
df_2 = pd.get_dummies(df[dummies])


# In[26]:


df = pd.concat([df_1.reset_index(drop=True), df_2], axis=1)
df.info()


# ### TABLA 4

# In[27]:


print("\t-> Reagrupar en tres variables Inspection Summary Result: reason, result_1 y result_2")


# In[28]:


df['inspection_summary_result'] = df['inspection_summary_result'].astype('str')


# In[29]:


df_3 = pd.DataFrame(df.inspection_summary_result.str.split('_-_',1).tolist(), columns= ['reason', 'result'])


# In[30]:


df_3['result'] = df_3['result'].astype('str')


# In[31]:


df_4 = pd.DataFrame(df_3.result.str.split(';_',1).tolist(), columns = ['result_1', 'result_2'])


# In[32]:


df_3 = df_3.drop(df_3.columns[[1]], axis=1) 


# In[33]:


df = pd.concat([df, df_3, df_4], axis=1)


# In[34]:


df = df.drop(df.columns[[33]], axis = 1) #Eliminar inspection_summary_result


# In[35]:


df.info()


# In[36]:


print("\t-> A la variable reason la hacemos dummy, es decir, initial annual inspection es 1 y en otro caso es cero")


# In[37]:


df.reason.value_counts(dropna=False)


# In[40]:


df['initial_annual_inspection'] = df.reason.apply(lambda x: 1 if x == "initial_annual_inspection" else 0)


# In[41]:


df.initial_annual_inspection.value_counts(dropna=False)


# In[42]:


df.info()


# In[43]:


df = df.drop(df.columns[[45]], axis=1) #Eliminamos la variable reason


# In[44]:


print("\t-> Creamos dummies a las variables result_1 y result_2")


# In[45]:


dummies = ["result_1", "result_2"]
df_2 = df[[i for i in df.keys() if i not in dummies]]
df_3 = pd.get_dummies(df[dummies])


# In[46]:


df = pd.concat([df_2.reset_index(drop=True), df_3], axis=1)
df.info()


# In[47]:


print("\t-> Creamos variables de año, mes y día a partir de Inspection date")


# In[48]:


df['inspection_date'] = pd.to_datetime(df.inspection_date, format = '%m/%d/%Y')


# In[49]:


df['inspection_year'] = df['inspection_date'].dt.year


# In[50]:


df['inspection_month_name'] = df['inspection_date'].dt.month_name()


# In[51]:


df['inspection_day_name'] = df['inspection_date'].dt.day_name()


# In[52]:


print("\t-> Eliminamos días festivos, sábado y domingo ")


# In[53]:


df = df.drop(df.loc[df['inspection_day_name']== 'Saturday'].index)


# In[54]:


df = df.drop(df.loc[df['inspection_day_name']== 'Sunday'].index)


# In[55]:


print("\t-> Poner como primer columna center_id e inspection_date")


# In[56]:


df.rename(columns={'day_care_id':'center_id'}, inplace=True)


# In[57]:


def order(frame,var): 
    varlist =[w for w in frame.columns if w not in var] 
    frame = frame[var+varlist] 
    return frame


# In[58]:


df = order(df,['center_id', 'inspection_date'])


# In[59]:


print("\t-> Ordenamos la base por year, month y day en forma descendente")


# In[60]:


df.sort_values(['inspection_date'], ascending=[False], inplace=True)


# In[61]:


print("\t-> Creamos dummy = 1 si existió violación")


# In[62]:


df.violation_category.value_counts(dropna=False)


# In[63]:


df['violation'] = df['violation_category'].apply(lambda x: not pd.isnull(x))


# In[64]:


df['violation'] = df['violation'].apply(lambda x: 1 if x == True else 0)


# In[65]:


df.violation.value_counts(dropna=False)


# In[66]:


print("\t-> Creamos dummy = 1 si existió violación y es un problema de salud pública")


# In[67]:


df['public_hazard'] = df['violation_category'].apply(lambda x: 1 if x == 'public_health_hazard' else 0)


# In[68]:


df.public_hazard.value_counts(dropna=False)


# In[69]:


print("\t-> Creamos la variable violaciones_hist_salud_publica: Número de violaciones de salud pública históricas (2016-2019) por centro")


# In[70]:


df['violaciones_hist_salud_publica'] = df.public_hazard[(df.inspection_year != 2020)]


# In[71]:


df_4 = df.groupby('center_id').violaciones_hist_salud_publica.sum().reset_index()


# In[72]:


df = pd.merge(left=df,right=df_4, how='left', left_on='center_id', right_on='center_id')


# In[73]:


df.info()


# In[74]:


df = df.drop(df.columns[[61]], axis=1) #Eliminamos la variable repetida


# In[75]:


df.rename(columns={'violaciones_hist_salud_publica_y':'violaciones_hist_salud_publica'}, inplace=True)


# In[76]:


print("\t-> Creamos la variable violaciones_2019_salud_publica: Número de violaciones de salud pública en el 2019 por centro")


# In[77]:


df['violaciones_2019_salud_publica'] = df.public_hazard[(df.inspection_year == 2019)]


# In[78]:


df_5 = df.groupby('center_id').violaciones_2019_salud_publica.sum().reset_index()


# In[79]:


df = pd.merge(left=df,right=df_5, how='left', left_on='center_id', right_on='center_id')


# In[80]:


df.info()


# In[81]:


df = df.drop(df.columns[[62]], axis=1) #Eliminamos la variable repetida


# In[82]:


df.rename(columns={'violaciones_2019_salud_publica_y':'violaciones_2019_salud_publica'}, inplace=True)


# In[83]:


print("\t-> Creamos la variable violaciones_hist_criticas: Número de violaciones críticas históricas anteriores (2016-2019) por centro")


# In[84]:


df['violation_critical'] = df['violation_category'].apply(lambda x: 1 if x == 'critical' else 0)


# In[85]:


df['violaciones_hist_criticas'] = df.violation_critical[(df.inspection_year != 2020)]


# In[86]:


df_6 = df.groupby('center_id').violaciones_hist_criticas.sum().reset_index()


# In[87]:


df = pd.merge(left=df,right=df_6, how='left', left_on='center_id', right_on='center_id')


# In[88]:


df.info()


# In[89]:


df = df.drop(df.columns[[64]], axis=1) #Eliminamos la variable repetida


# In[90]:


df.rename(columns={'violaciones_hist_criticas_y':'violaciones_hist_criticas'}, inplace=True)


# In[91]:


print("\t-> Creamos la variable violaciones_2019_criticas: Número de violaciones críticas en el 2019 por centro")


# In[92]:


df['violaciones_2019_criticas'] = df.violation_critical[(df.inspection_year == 2019)]


# In[93]:


df_7 = df.groupby('center_id').violaciones_2019_criticas.sum().reset_index()


# In[94]:


df = pd.merge(left=df,right=df_7, how='left', left_on='center_id', right_on='center_id')


# In[95]:


df.info()


# In[96]:


df = df.drop(df.columns[[65]], axis=1) #Eliminamos la variable repetida


# In[97]:


df.rename(columns={'violaciones_2019_criticas_y':'violaciones_2019_criticas'}, inplace=True)


# In[98]:


print("\t-> Creamos la variable ratio_violaciones_hist: Número de inspecciones en total de primera vez que resultaron en violación crítica o de salud pública/ número de inspecciones de primera vez por centro")


# In[99]:


df_8 = df.loc[df['inspection_year'] != 2020]


# In[100]:


df_8 = df[df.violation_category.isin(['critical', 'public_health_hazard']) & df['initial_annual_inspection']==1]


# In[101]:


df_9 = df_8.groupby('center_id').initial_annual_inspection.sum().reset_index()


# In[102]:


df_10 = df.groupby('center_id').initial_annual_inspection.sum().reset_index()


# In[103]:


df_11 = pd.merge(left=df_10,right=df_9, how='left', left_on='center_id', right_on='center_id')


# In[104]:


df_11['ratio_violaciones_hist'] = df_11['initial_annual_inspection_y'] / df_11['initial_annual_inspection_x']


# In[105]:


df = pd.merge(left=df,right=df_11, how='left', left_on='center_id', right_on='center_id')


# In[106]:


df.info()


# In[107]:


df = df.drop(df.columns[[66,67]], axis=1) #Eliminamos variables que no necesitamos 


# In[108]:


print("\t-> Creamos la variable ratio_violaciones_2019: Número de inspecciones en total de primera vez que resultaron en violación crítica o de salud pública en el 2019 / número de inspecciones de primera vez por centro")


# In[109]:


df_12 = df.loc[df['inspection_year'] == 2019]


# In[110]:


df_12 = df[df.violation_category.isin(['critical', 'public_health_hazard']) & df['initial_annual_inspection']==1]


# In[111]:


df_13 = df_12.groupby('center_id').initial_annual_inspection.sum().reset_index()


# In[112]:


df_14 = pd.merge(left=df_10,right=df_13, how='left', left_on='center_id', right_on='center_id')


# In[113]:


df_14['ratio_violaciones_2019'] = df_14['initial_annual_inspection_y'] / df_14['initial_annual_inspection_x']


# In[114]:


df = pd.merge(left=df,right=df_14, how='left', left_on='center_id', right_on='center_id')


# In[115]:


df.info()


# In[116]:


df = df.drop(df.columns[[67,68]], axis=1) #Eliminamos variables que no necesitamos 

