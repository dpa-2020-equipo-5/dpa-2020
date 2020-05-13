# -*- coding: utf-8 -*-
"""
Created on Wed May 13 01:04:04 2020

@author: Elizabeth
"""

print("\t-> RandomForest:")

from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics

rforest = RandomForestClassifier(n_estimators=600, class_weight="balanced", max_depth=8, criterion='gini')

rforest.fit(X_train,Y_train.values.ravel())

Y_predf = rforest.predict(X_test)

print("Precision:",metrics.precision_score(Y_test, Y_predf, average='macro'))

print("Recall:",metrics.recall_score(Y_test, Y_predf, average='macro'))

rforest_matrix=metrics.confusion_matrix(Y_test,Y_predf)

feature_importance_frame = pd.DataFrame()
feature_importance_frame['features'] = list(X_train.keys())
feature_importance_frame['importance'] = list(rforest.feature_importances_)
feature_importance_frame = feature_importance_frame.sort_values(
        'importance', ascending=False)
feature_importance_frame
