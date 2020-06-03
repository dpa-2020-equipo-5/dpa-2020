import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from .data_preparator import DataPreparator
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters


class NYCCCCIRandomForest:
    def __init__(self, year, month, day):
        self.task_id = "{}{}{}".format(str(year), str(month), str(day))
        host, database, user, password = get_database_connection_parameters()
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)

    def fit(self):
        df = pd.read_sql("select * from modeling.model_parameters where task_id='{}'".format(self.task_id), self.engine)
        
        rforest = RandomForestClassifier(
            bootstrap=bool(df['bootstrap'].values[0]), 
            n_estimators= int(df['n_estimators'].values[0]), 
            class_weight=str(df['class_weight'].values[0]),
            max_depth=int(df['max_depth'].values[0]), 
            criterion=str(df['criterion'].values[0]),
        )
        data_prep = DataPreparator()
        X_train, y_train, X_test, y_test = data_prep.split_train_test()
        rforest.fit(X_train.values, y_train.values.ravel())
        return rforest