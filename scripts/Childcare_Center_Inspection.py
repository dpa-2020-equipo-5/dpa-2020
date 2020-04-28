#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns #Control figure 
import numpy as np
import os
from datetime import date
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


encoded_columns = pd.get_dummies(df['borough'])


# In[26]:


df = df.join(encoded_columns)


# In[27]:


dummies = ["status", "program_type", "facility_type"]
df_1 = df[[i for i in df.keys() if i not in dummies]]
df_2 = pd.get_dummies(df[dummies])


# In[28]:


df = pd.concat([df_1.reset_index(drop=True), df_2], axis=1)
df.info()


# ### TABLA 4

# In[29]:


print("\t-> Reagrupar en tres variables Inspection Summary Result: reason, result_1 y result_2")


# In[30]:


df['inspection_summary_result'] = df['inspection_summary_result'].astype('str')


# In[31]:


df_3 = pd.DataFrame(df.inspection_summary_result.str.split('_-_',1).tolist(), columns= ['reason', 'result'])


# In[32]:


df_3['result'] = df_3['result'].astype('str')


# In[33]:


df_4 = pd.DataFrame(df_3.result.str.split(';_',1).tolist(), columns = ['result_1', 'result_2'])


# In[34]:


df_3 = df_3.drop(df_3.columns[[1]], axis=1) 


# In[35]:


df = pd.concat([df, df_3, df_4], axis=1)


# In[36]:


df = df.drop(['inspection_summary_result'], axis = 1) #Eliminar inspection_summary_result


# In[37]:


print("\t-> A la variable reason la hacemos dummy, es decir, initial annual inspection es 1 y en otro caso es cero")


# In[38]:


df.reason.value_counts(dropna=False)


# In[39]:


df['initial_annual_inspection'] = df.reason.apply(lambda x: 1 if x == "initial_annual_inspection" else 0)


# In[40]:


df.initial_annual_inspection.value_counts(dropna=False)


# In[41]:


df = df.drop(['reason'], axis=1) #Eliminamos la variable reason


# In[42]:


print("\t-> Creamos dummies a las variables result_1 y result_2")


# In[43]:


dummies = ["result_1", "result_2"]
df_2 = df[[i for i in df.keys() if i not in dummies]]
df_3 = pd.get_dummies(df[dummies])


# In[44]:


df = pd.concat([df_2.reset_index(drop=True), df_3], axis=1)
df.info()


# In[45]:


print("\t-> Creamos variables de año, mes y día a partir de Inspection date")


# In[46]:


df['inspection_date'] = pd.to_datetime(df.inspection_date, format = '%m/%d/%Y')


# In[47]:


df['inspection_year'] = df['inspection_date'].dt.year


# In[48]:


df['inspection_month_name'] = df['inspection_date'].dt.month_name()


# In[49]:


df['inspection_day_name'] = df['inspection_date'].dt.day_name()


# In[50]:


print("\t-> Eliminamos días festivos, sábado y domingo ")


# In[51]:


df = df.drop(df.loc[df['inspection_day_name']== 'Saturday'].index)


# In[52]:


df = df.drop(df.loc[df['inspection_day_name']== 'Sunday'].index)


# In[53]:


print("\t-> Poner como primer columna center_id e inspection_date")


# In[54]:


df.rename(columns={'day_care_id':'center_id'}, inplace=True)


# In[55]:


def order(frame,var): 
    varlist =[w for w in frame.columns if w not in var] 
    frame = frame[var+varlist] 
    return frame


# In[56]:


df = order(df,['center_id', 'inspection_date'])


# In[57]:


print("\t-> Ordenamos la base por year, month y day en forma descendente")


# In[58]:


df.sort_values(['inspection_date'], ascending=[False], inplace=True)


# In[59]:


print("\t-> Creamos dummy = 1 si existió violación")


# In[60]:


df.violation_category.value_counts(dropna=False)


# In[61]:


df['violation'] = df['violation_category'].apply(lambda x: not pd.isnull(x))


# In[62]:


df['violation'] = df['violation'].apply(lambda x: 1 if x == True else 0)


# In[63]:


df.violation.value_counts(dropna=False)


# In[64]:


print("\t-> Creamos dummy = 1 si existió violación y es un problema de salud pública")


# In[65]:


df['public_hazard'] = df['violation_category'].apply(lambda x: 1 if x == 'public_health_hazard' else 0)


# In[66]:


df.public_hazard.value_counts(dropna=False)


# In[67]:


print("\t-> Creamos la variable violaciones_hist_salud_publica: Número de violaciones de salud pública históricas (2016-2019) por centro")


# In[68]:


df['violaciones_hist_salud_publica'] = df.public_hazard[(df.inspection_year != 2020)]


# In[69]:


df_4 = df.groupby('center_id').violaciones_hist_salud_publica.sum().reset_index()


# In[70]:


df = pd.merge(left=df,right=df_4, how='left', left_on='center_id', right_on='center_id')


# In[71]:


df = df.drop(['violaciones_hist_salud_publica_x'], axis=1) #Eliminamos la variable repetida


# In[72]:


df.rename(columns={'violaciones_hist_salud_publica_y':'violaciones_hist_salud_publica'}, inplace=True)


# In[73]:


print("\t-> Creamos la variable violaciones_2019_salud_publica: Número de violaciones de salud pública en el 2019 por centro")


# In[74]:


df['violaciones_2019_salud_publica'] = df.public_hazard[(df.inspection_year == 2019)]


# In[75]:


df_5 = df.groupby('center_id').violaciones_2019_salud_publica.sum().reset_index()


# In[76]:


df = pd.merge(left=df,right=df_5, how='left', left_on='center_id', right_on='center_id')


# In[77]:


df = df.drop(['violaciones_2019_salud_publica_x'], axis=1) #Eliminamos la variable repetida


# In[78]:


df.rename(columns={'violaciones_2019_salud_publica_y':'violaciones_2019_salud_publica'}, inplace=True)


# In[79]:


print("\t-> Creamos la variable violaciones_hist_criticas: Número de violaciones críticas históricas anteriores (2016-2019) por centro")


# In[80]:


df['violation_critical'] = df['violation_category'].apply(lambda x: 1 if x == 'critical' else 0)


# In[81]:


df['violaciones_hist_criticas'] = df.violation_critical[(df.inspection_year != 2020)]


# In[82]:


df_6 = df.groupby('center_id').violaciones_hist_criticas.sum().reset_index()


# In[83]:


df = pd.merge(left=df,right=df_6, how='left', left_on='center_id', right_on='center_id')


# In[84]:


df = df.drop(['violaciones_hist_criticas_x'], axis=1) #Eliminamos la variable repetida


# In[85]:


df.rename(columns={'violaciones_hist_criticas_y':'violaciones_hist_criticas'}, inplace=True)


# In[86]:


print("\t-> Creamos la variable violaciones_2019_criticas: Número de violaciones críticas en el 2019 por centro")


# In[87]:


df['violaciones_2019_criticas'] = df.violation_critical[(df.inspection_year == 2019)]


# In[88]:


df_7 = df.groupby('center_id').violaciones_2019_criticas.sum().reset_index()


# In[89]:


df = pd.merge(left=df,right=df_7, how='left', left_on='center_id', right_on='center_id')


# In[90]:


df = df.drop(['violaciones_2019_criticas_x'], axis=1) #Eliminamos la variable repetida


# In[91]:


df.rename(columns={'violaciones_2019_criticas_y':'violaciones_2019_criticas'}, inplace=True)


# In[92]:


print("\t-> Creamos la variable ratio_violaciones_hist: Número de inspecciones en total de primera vez que resultaron en violación crítica o de salud pública/ número de inspecciones de primera vez por centro")


# In[93]:


df_8 = df.loc[df['inspection_year'] != 2020]


# In[94]:


df_9 = df_8[df_8.violation_category.isin(['critical', 'public_health_hazard']) & df_8['initial_annual_inspection']==1]


# In[95]:


df_10 = df_9.groupby('center_id').initial_annual_inspection.sum().reset_index()


# In[96]:


df_11 = df.groupby('center_id').initial_annual_inspection.sum().reset_index()


# In[97]:


df_12 = pd.merge(left=df_11,right=df_10, how='left', left_on='center_id', right_on='center_id')


# In[98]:


df_12['ratio_violaciones_hist'] = df_12['initial_annual_inspection_y'] / df_12['initial_annual_inspection_x']


# In[99]:


df = pd.merge(left=df,right=df_12, how='left', left_on='center_id', right_on='center_id')


# In[100]:


df = df.drop(['initial_annual_inspection_x', 'initial_annual_inspection_y'], axis=1) #Eliminamos variables que no necesitamos 


# In[101]:


print("\t-> Creamos la variable ratio_violaciones_2019: Número de inspecciones en total de primera vez que resultaron en violación crítica o de salud pública en el 2019 / número de inspecciones de primera vez por centro")


# In[102]:


df_13 = df.loc[df['inspection_year'] == 2019]


# In[103]:


df_14 = df_13[df_13.violation_category.isin(['critical', 'public_health_hazard']) & df_13['initial_annual_inspection']==1]


# In[104]:


df_15 = df_14.groupby('center_id').initial_annual_inspection.sum().reset_index()


# In[105]:


df_16 = pd.merge(left=df_11,right=df_15, how='left', left_on='center_id', right_on='center_id')


# In[106]:


df_16['ratio_violaciones_2019'] = df_16['initial_annual_inspection_y'] / df_16['initial_annual_inspection_x']


# In[107]:


df = pd.merge(left=df,right=df_16, how='left', left_on='center_id', right_on='center_id')


# In[108]:


df = df.drop(['initial_annual_inspection_x','initial_annual_inspection_y'], axis=1) #Eliminamos variables que no necesitamos 


# In[109]:


print("\t-> Creamos la variable prom_violaciones_hist_borough: Promedio de violaciones históricas por distrito")


# In[110]:


df_17 = df.loc[df['inspection_year'] != 2020]


# In[111]:


df_18 = df_17.groupby('borough').violation.mean().reset_index()


# In[112]:


df = pd.merge(left=df,right=df_18, how='left', left_on='borough', right_on='borough')


# In[113]:


df.rename(columns={'violation_y':'prom_violaciones_hist_borough'}, inplace=True)


# In[114]:


df.rename(columns={'violation_x':'violation'}, inplace=True)


# In[115]:


print("\t-> Creamos la variable prom_violaciones_2019_borough: Promedio de violaciones en el 2019 por distrito")


# In[116]:


df_19 = df.loc[df['inspection_year'] == 2019]


# In[117]:


df_20 = df_19.groupby('borough').violation.mean().reset_index()


# In[118]:


df = pd.merge(left=df,right=df_20, how='left', left_on='borough', right_on='borough')


# In[119]:


df.rename(columns={'violation_y':'prom_violaciones_2019_borough'}, inplace=True)


# In[120]:


df.rename(columns={'violation_x':'violation'}, inplace=True)


# In[121]:


print("\t-> Creamos la variable ratio_violaciones_hist_sp: Número de violaciones de salud pública de primera vez por centro históricas (2017-2019)/ número de violaciones de primera vez de todo tipo por centro históricas (2017-2019) ")


# In[122]:


df_21 = df.loc[df['inspection_year'] != 2020]


# In[123]:


df_22 = df_21.loc[df_21['initial_annual_inspection'] == 1]


# In[124]:


df_23 = df_22.groupby('center_id').public_hazard.sum().reset_index()


# In[125]:


df_24 = df_22.groupby('center_id').violation.sum().reset_index()


# In[126]:


df_25 = pd.merge(left=df_23,right=df_24, how='left', left_on='center_id', right_on='center_id')


# In[127]:


df_25['ratio_violaciones_hist_sp'] = df_25['public_hazard'] / df_25['violation']


# In[128]:


df = pd.merge(left=df,right=df_25, how='left', left_on='center_id', right_on='center_id')


# In[129]:


df = df.drop(['public_hazard_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 


# In[130]:


df.rename(columns={'violation_x':'violation'}, inplace=True)


# In[131]:


df.rename(columns={'public_hazard_x':'public_hazard'}, inplace=True)


# In[132]:


print("\t-> Creamos la variable ratio_violaciones_2019_sp: Número de violaciones de salud pública de primera vez por centro en el 2019 / número de violaciones de primera vez de todo tipo por centro en el 2019 ")


# In[133]:


df_26 = df.loc[df['inspection_year'] == 2019]


# In[134]:


df_27 = df_26.loc[df_26['initial_annual_inspection'] == 1]


# In[135]:


df_28 = df_27.groupby('center_id').public_hazard.sum().reset_index()


# In[136]:


df_29 = df_27.groupby('center_id').violation.sum().reset_index()


# In[137]:


df_30 = pd.merge(left=df_28,right=df_29, how='left', left_on='center_id', right_on='center_id')


# In[138]:


df_30['ratio_violaciones_2019_sp'] = df_30['public_hazard'] / df_30['violation']


# In[139]:


df = pd.merge(left=df,right=df_30, how='left', left_on='center_id', right_on='center_id')


# In[140]:


df = df.drop(['public_hazard_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 


# In[141]:


df.rename(columns={'violation_x':'violation'}, inplace=True)


# In[142]:


df.rename(columns={'public_hazard_x':'public_hazard'}, inplace=True)


# In[143]:


print("\t-> Creamos la variable ratio_violaciones_hist_criticas: Número de violaciones críticas de primera vez por centro históricas (2017-2019)/ número de violaciones de primera vez de todo tipo por centro históricas (2017-2019)")


# In[144]:


df_31 = df.loc[df['inspection_year'] != 2020]


# In[145]:


df_32 = df_31.loc[df_31['initial_annual_inspection'] == 1]


# In[146]:


df_33 = df_32.groupby('center_id').violation_critical.sum().reset_index()


# In[147]:


df_34 = df_32.groupby('center_id').violation.sum().reset_index()


# In[148]:


df_35 = pd.merge(left=df_33,right=df_34, how='left', left_on='center_id', right_on='center_id')


# In[149]:


df_35['ratio_violaciones_hist_criticas'] = df_35['violation_critical'] / df_35['violation']


# In[150]:


df = pd.merge(left=df,right=df_35, how='left', left_on='center_id', right_on='center_id')


# In[151]:


df = df.drop(['violation_critical_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 


# In[152]:


df.rename(columns={'violation_x':'violation'}, inplace=True)


# In[153]:


df.rename(columns={'violation_critical_x':'violation_critical'}, inplace=True)


# In[154]:


print("\t-> Creamos la variable ratio_violaciones_2019_criticas: Número de violaciones críticas de primera vez por centro en el 2019/ número de violaciones de primera vez de todo tipo por centro en el 2019")


# In[155]:


df_36 = df.loc[df['inspection_year'] == 2019]


# In[156]:


df_37 = df_36.loc[df_36['initial_annual_inspection'] == 1]


# In[157]:


df_38 = df_37.groupby('center_id').violation_critical.sum().reset_index()


# In[158]:


df_39 = df_37.groupby('center_id').violation.sum().reset_index()


# In[159]:


df_40 = pd.merge(left=df_38,right=df_39, how='left', left_on='center_id', right_on='center_id')


# In[160]:


df_40['ratio_violaciones_2019_criticas'] = df_40['violation_critical'] / df_40['violation']


# In[161]:


df = pd.merge(left=df,right=df_40, how='left', left_on='center_id', right_on='center_id')


# In[162]:


df = df.drop(['violation_critical_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 


# In[163]:


df.rename(columns={'violation_x':'violation'}, inplace=True)


# In[164]:


df.rename(columns={'violation_critical_x':'violation_critical'}, inplace=True)


# In[165]:


df.info()


# In[166]:


df = df.set_index(['center_id', 'inspection_date'])


# In[167]:


df = df.drop(df.columns[[0,1,2,3,4,5,6,7,8,9,11,12,13,14,15,17,19,21,23,24,25,26,27,56,57]], axis=1) #Eliminamos variables que no necesitamos 


# In[168]:


df = df.fillna(0)


# Modelo Random Forest

# - Para el entrenamiento se usaron todos los datos del 2017-2019 y para validación los datos correspondientes a lo que va del año 2020.

# - Mediante una gráfica de barras se verifica si la muestra esta balanceada o no y se observa que no está balanceada.

# In[169]:


sns.countplot(x='public_hazard', data=df, palette="Set3")


# - Así que se utiliza _over-sampling_ para balancear la muestra.

# In[170]:


count_class_0, count_class_1 = df.public_hazard.value_counts()


# In[171]:


df_class_0 = df[df['public_hazard'] == 0]
df_class_1 = df[df['public_hazard'] == 1]


# In[172]:


count_class_0


# In[173]:


count_class_1


# In[174]:


df_class_0_over = df_class_0.sample(count_class_1, replace=True)
df_test_over = pd.concat([df_class_1, df_class_0_over], axis=0)
df_test_over.head()


# In[175]:


print('Random over-sampling:')
print(df_test_over.public_hazard.value_counts())

df_test_over.public_hazard.value_counts().plot(kind='bar', title='Count (public_hazard)');


# In[176]:


df_train = df_test_over.loc[df_test_over['inspection_year'] != 2020] 


# In[177]:


df_test = df_test_over.loc[df_test_over['inspection_year'] == 2020]


# In[178]:


Y_train = df_train[['public_hazard']]


# In[179]:


Y_test = df_test[['public_hazard']]


# In[180]:


X_train = df_train[[i for i in df_train.keys() if i not in Y_train]]


# In[181]:


X_test = df_test[[i for i in df_test.keys() if i not in Y_test]]


# In[182]:


import sklearn as sk
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.metrics import mean_squared_error


# In[183]:


np.random.seed(0)
rforest = RandomForestClassifier(n_estimators=600, class_weight="balanced", max_depth=8, criterion='gini')
rforest.fit(X_train,Y_train.values.ravel())


# In[184]:


Y_pred = rforest.predict(X_test)


# In[185]:


print("Accuracy:",metrics.accuracy_score(Y_test, Y_pred))
print("Precision:",metrics.precision_score(Y_test, Y_pred, average='macro'))
print("Recall:",metrics.recall_score(Y_test, Y_pred, average='macro'))


# In[186]:


print("Mean squared error: %.2f" % mean_squared_error(Y_test, Y_pred))


# In[187]:


rforest_matrix=metrics.confusion_matrix(Y_test,Y_pred)


# In[188]:


pd.DataFrame(rforest_matrix)


# In[189]:


class_names=[0,1] 
fig, ax = plt.subplots()
tick_marks = np.arange(len(class_names))
plt.xticks(tick_marks, class_names)
plt.yticks(tick_marks, class_names)
sns.heatmap(pd.DataFrame(rforest_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
ax.xaxis.set_label_position("top")
plt.tight_layout()
plt.title('Confusion matrix', y=1.1)
plt.ylabel('Actual label')
plt.xlabel('Predicted label')


# In[190]:


feature_importance_frame = pd.DataFrame()
feature_importance_frame['features'] = list(X_train.keys())
feature_importance_frame['importance'] = list(rforest.feature_importances_)
feature_importance_frame = feature_importance_frame.sort_values(
        'importance', ascending=False)
feature_importance_frame


# Modelo XGBoost

# In[191]:


import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score
from collections import Counter
from sklearn.datasets import make_classification
import multiprocessing


# In[192]:


xg_clas = xgb.XGBClassifier(n_estimators=500, max_depth=3, learning_rate=0.01, subsample=1, objective='binary:logistic', booster='gbtree', n_jobs=1, nthread=multiprocessing.cpu_count())


# In[193]:


xg_clas.fit(X_train, Y_train)


# In[194]:


Y_p = xg_clas.predict(X_test)


# In[195]:


rmse = np.sqrt(mean_squared_error(Y_test, Y_p))
print("RMSE: %f" % (rmse))


# In[196]:


print("Mean squared error: %.2f" % mean_squared_error(Y_test, Y_p))


# In[197]:


cnf_matrix = metrics.confusion_matrix(Y_test, Y_p)


# In[198]:


pd.DataFrame(cnf_matrix)


# In[199]:


class_names=[0,1] 
fig, ax = plt.subplots()
tick_marks = np.arange(len(class_names))
plt.xticks(tick_marks, class_names)
plt.yticks(tick_marks, class_names)
sns.heatmap(pd.DataFrame(cnf_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
ax.xaxis.set_label_position("top")
plt.tight_layout()
plt.title('Confusion matrix', y=1.1)
plt.ylabel('Actual label')
plt.xlabel('Predicted label')


# In[200]:


feature_importance_frame = pd.DataFrame()
feature_importance_frame['features'] = list(X_train.keys())
feature_importance_frame['importance'] = list(xg_clas.feature_importances_)
feature_importance_frame = feature_importance_frame.sort_values(
        'importance', ascending=False)
feature_importance_frame

