import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns #Control figure 
import numpy as np
import os
from datetime import date
matplotlib.style.use('ggplot')
%matplotlib inline
from sodapy import Socrata

#Uniendo las tablas 3 y 4
tabla_5 = tabla_4.join(tabla_3, lsuffix='_caller', rsuffix='_other')
tabla_5 = tabla_5.set_index(['center_id', 'inspection_date'])

#Seleccionando las variables a usar en el modelo
tabla_5 = tabla_5.drop(tabla_5.columns[[0,1,2,3,4,17,18,35]], axis=1) #Eliminamos variables que no necesitamos 

#Llenando na's
tabla_5 = tabla_5.fillna(0)

#Oversampling
count_class_0, count_class_1 = tabla_5.public_hazard.value_counts()
df_class_0 = tabla_5[tabla_5['public_hazard'] == 0]
df_class_1 = tabla_5[tabla_5['public_hazard'] == 1]
df_class_0_over = df_class_0.sample(count_class_1, replace=True)
df_test_over = pd.concat([df_class_1, df_class_0_over], axis=0)

#Entrenamiento y validación
df_train = df_test_over.loc[df_test_over['inspection_year'] != 2020]
df_test = df_test_over.loc[df_test_over['inspection_year'] == 2020]
Y_train = df_train[['public_hazard']]
Y_test = df_test[['public_hazard']]
X_train = df_train[[i for i in df_train.keys() if i not in Y_train]]
X_test = df_test[[i for i in df_test.keys() if i not in Y_test]]

import sklearn as sk
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.metrics import mean_squared_error

np.random.seed(0)
rforest = RandomForestClassifier(n_estimators=600, class_weight="balanced", max_depth=8, criterion='gini')
rforest.fit(X_train,Y_train.values.ravel())
Y_pred = rforest.predict(X_test)

print("Accuracy:",metrics.accuracy_score(Y_test, Y_pred))
print("Precision:",metrics.precision_score(Y_test, Y_pred, average='macro'))
print("Recall:",metrics.recall_score(Y_test, Y_pred, average='macro'))
print("Mean squared error: %.2f" % mean_squared_error(Y_test, Y_pred))

rforest_matrix=metrics.confusion_matrix(Y_test,Y_pred)

pd.DataFrame(rforest_matrix)

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

feature_importance_frame = pd.DataFrame()
feature_importance_frame['features'] = list(X_train.keys())
feature_importance_frame['importance'] = list(rforest.feature_importances_)
feature_importance_frame = feature_importance_frame.sort_values(
        'importance', ascending=False)
feature_importance_frame






















