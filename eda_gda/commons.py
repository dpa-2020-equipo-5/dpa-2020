#Autor: Gerardo Mathus - @gemathus
#Autor: Karla Alfaro - @alpika
#oct 2019

def clean_string(astr):
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

def clean_columns(df):
    for series in df:
        df.rename(columns={series:clean_string(series)}, inplace=True)