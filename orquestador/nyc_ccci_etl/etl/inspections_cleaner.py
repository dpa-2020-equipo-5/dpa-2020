import pandas as pd
import numpy as np
import json
class InspectionsCleaner():
    def __init__(self, raw_json_data):
        self.raw_json_data = raw_json_data

    def clean_string(self, astr):
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
    
    def clean_columns(self, df):
        for series in df:
            df.rename(columns={series: self.clean_string(series)}, inplace=True)

    def execute(self):
        df = pd.read_json(json.dumps(self.raw_json_data))
        
        self.clean_columns(df)
        for col in df.select_dtypes('object'):
            df[col] = df[col].replace('\\s+', ' ', regex=True)
        
        for col in df.select_dtypes('object'):
            df[col] = df[col].str.strip()
            df[col] = df[col].str.lower()
            df[col] = df[col].str.replace('á', 'a')
            df[col] = df[col].str.replace('é', 'e')
            df[col] = df[col].str.replace('í', 'i')
            df[col] = df[col].str.replace('ó', 'o')
            df[col] = df[col].str.replace('ú', 'u')
            df[col] = df[col].str.replace(' ', '_')
            df[col] = df[col].str.replace('-', '_')

        for col in df.select_dtypes('object'):
            df.loc[df[col] == 'na', col] = np.nan
            df.loc[df[col] == 'n/a', col] = np.nan

        df = df.drop_duplicates()
        if len(df) > 0:
            return [tuple(x) for x in df.to_numpy()], list(df.columns)
        else:
            return [], ()
