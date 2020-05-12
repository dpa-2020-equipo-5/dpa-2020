# -*- coding: utf-8 -*-
"""
Created on Tue May 12 07:27:12 2020

@author: Elizabeth
"""

print("\t-> XGBoost:")

import xgboost as xgb
from xgboost import XGBClassifier
from sklearn import metrics
from sklearn.model_selection import GridSearchCV
import multiprocessing

xgb_class = xgb.XGBClassifier(n_estimators=1000, max_depth=4, booster='gbtree', min_child_weight=6, gamma=0, eta=0.5, subsample=0.8, learning_rate=0.1, objective='binary:logistic', n_jobs=1, eval_metric=['auc', 'aucpr', 'error'], nthread=multiprocessing.cpu_count())

xgb_class.fit(X_train, Y_train, early_stopping_rounds=10, eval_set=[(X_test, Y_test)])

Y_pred = pd.DataFrame(xgb_class.predict(X_test))

cnf_matrix = metrics.confusion_matrix(Y_test, Y_pred)

print("Precision:",metrics.precision_score(Y_test, Y_pred, average='macro'))

print("Recall:",metrics.recall_score(Y_test, Y_pred, average='macro'))

xgb.plot_importance(xgb_class)

feature_importance_frame = pd.DataFrame()
feature_importance_frame['features'] = list(X_train.keys())
feature_importance_frame['importance'] = list(xgb_class.feature_importances_)
feature_importance_frame = feature_importance_frame.sort_values(
        'importance', ascending=False)
feature_importance_frame