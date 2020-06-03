import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.model_selection import GridSearchCV

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

from sklearn.model_selection import train_test_split
from nyc_ccci_etl.model.data_preparator import DataPreparator
class RandomForestGridSearch:
    
    @staticmethod
    def find_best_params():
        data_prep = DataPreparator()
        X, y, x_test, y_test = data_prep.split_train_test()

        rforest = RandomForestClassifier()

        hyper_param_grid = {
            'n_estimators': [1000], 
            'max_depth': [5,10], 
            'criterion':['gini'],
            'class_weight':["balanced"],
            'bootstrap':[True],
        }

        grid_search = GridSearchCV(
            rforest, 
            hyper_param_grid, 
            scoring = 'f1',
            cv = 10, 
            n_jobs = -1,
            verbose = 3)

        grid_search.fit(X, y.values.ravel())

        print("="*100)
        print("best_params: ")
        print(grid_search.best_params_)
        
        cv_results = pd.DataFrame(grid_search.cv_results_)
        print("="*100)
        print("cv_results: ")
        print(cv_results.head())

        return grid_search.best_params_, grid_search.best_score_
