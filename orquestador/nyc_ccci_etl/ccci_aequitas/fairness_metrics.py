import pandas as pd
import boto3
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.commons.configuration import get_aws_bucket
from aequitas.group import Group
from aequitas.fairness import Fairness
from aequitas.bias import Bias
from io import BytesIO
import pickle
class FairnessMetrics:
    def __init__(self, year, month, day):
        host, database, user, password = get_database_connection_parameters()
        self.bucket = get_aws_bucket()
        self.date_param = "{}-{}-{}".format(year, str(month).zfill(2), str(day).zfill(2))
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)
    def get_lastest_model(self, session):
        s3_client = session.client('s3')
        response = s3_client.list_objects_v2(Bucket=self.bucket)
        all_models = response['Contents']
        latest = max(all_models, key=lambda x: x['LastModified'])
        return latest['Key']
    def download_model(self):
        ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        latest_model = self.get_lastest_model(ses)
        self.model_id = "s3://" + self.bucket + "/" + latest_model
        s3_resource = ses.resource('s3')
        with BytesIO() as data:
            s3_resource.Bucket(self.bucket).download_fileobj(latest_model, data)
            data.seek(0)
            model = pickle.load(data)
        return model

    def execeute(self):
        model = self.download_model()
        tabla_3 = pd.read_sql_table('centers', self.engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', self.engine, schema="transformed")

        centros = tabla_3.copy()
        centros.rename(columns={"dc_id":"center_id"}, inplace=True)
        inspecciones = tabla_4.copy()
        last_inspections = inspecciones.sort_values(by="inspectiondate").drop_duplicates(subset=["center_id"], keep="last")
        centros = centros.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent', 'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)
        centros = centros.reset_index(drop=True)
        tabla_5 = pd.merge(last_inspections, centros)
        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype(float)

        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_nan'], axis=1)

        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5.select_dtypes(object):
            tabla_5[col] = tabla_5[col].astype(float)

        tabla_5 = tabla_5.fillna(0)  

        prds = model.predict(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))
        probas = model.predict_proba(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))

        res = pd.DataFrame({
            "center":tabla_5.index,
            "etiqueta":prds,
            "proba_0":probas[:,0],
            "proba_1":probas[:,1]
        })

        res.loc[res['proba_0'] > res['proba_1'], 'score'] = res['proba_0']
        res.loc[res['proba_0'] < res['proba_1'], 'score'] = res['proba_1']

        categorias_1 = ["programtype_all_age_camp","programtype_infant_toddler","programtype_preschool", "programtype_preschool_camp", "programtype_school_age_camp"]
        programtype = pd.get_dummies(centros[categorias_1]).idxmax(1)
        categorias_2 = ["borough_bronx","borough_brooklyn","borough_manhattan", "borough_queens", "borough_staten_island"]
        borough = pd.get_dummies(centros[categorias_2]).idxmax(1)
        ambas = pd.concat([borough, programtype], axis=1,)
        ambas = ambas.rename(columns={0:'borough', 1:'programtype'})
        tabla_1 = pd.concat([centros, ambas], axis=1)
        tabla_2 = pd.merge(res, tabla_1, left_on='center', right_on='center_id')

        for i in list(tabla_2.index):
            if str(tabla_2.iloc[i].borough_bronx) == "1":
                tabla_2.loc[tabla_2.index == i ,"borough"] = "bronx"
            elif str(tabla_2.iloc[i].borough_brooklyn) == "1":
                tabla_2.loc[tabla_2.index == i ,"borough"] = "brooklyn"
            elif str(tabla_2.iloc[i].borough_manhattan) == "1":
                tabla_2.loc[tabla_2.index == i ,"borough"] = "manhattan"
            elif str(tabla_2.iloc[i].borough_queens) == "1":
                tabla_2.loc[tabla_2.index == i ,"borough"] = "queens"
            elif str(tabla_2.iloc[i].borough_staten_island) == "1":
                tabla_2.loc[tabla_2.index == i ,"borough"] = "staten_island"

        tabla_2.drop(categorias_2, axis=1, inplace=True)

        for i in list(tabla_2.index):
            if str(tabla_2.iloc[i].programtype_all_age_camp) == "1":
                tabla_2.loc[tabla_2.index == i ,"programtype"] = "all_age_camp"
            elif str(tabla_2.iloc[i].programtype_infant_toddler) == "1":
                tabla_2.loc[tabla_2.index == i ,"programtype"] = "infant_toddler"
            elif str(tabla_2.iloc[i].programtype_preschool) == "1":
                tabla_2.loc[tabla_2.index == i ,"programtype"] = "preschool"
            elif str(tabla_2.iloc[i].programtype_preschool_camp) == "1":
                tabla_2.loc[tabla_2.index == i ,"programtype"] = "preschool_camp"
            elif str(tabla_2.iloc[i].programtype_school_age_camp) == "1":
                tabla_2.loc[tabla_2.index == i ,"programtype"] = "school_age_camp"

        tabla_2.drop(categorias_1, axis=1, inplace=True)

        tabla_6 = tabla_2.loc[:, ['center', 'etiqueta', 'score', 'borough', 'programtype']]
        tabla_6 =  tabla_6.rename(columns = {'etiqueta':'label_value'})
        tabla_6.set_index('center', inplace=True)

        g = Group()
        xtab, _ = g.get_crosstabs(tabla_6)

        
        b = Bias()
        bdf = b.get_disparity_predefined_groups(xtab, original_df=tabla_6, ref_groups_dict={'borough':'brooklyn', 'programtype':'preschool'}, alpha=0.05, mask_significance=True)
        f = Fairness()
        fdf = f.get_group_value_fairness(bdf)


        fdf['model_id'] = self.model_id
        fdf['date'] = self.date_param
        self.output_table = fdf
        return [tuple(x) for x in fdf.to_numpy()], [(c.replace("for", "forr").replace(" ", "_"), 'VARCHAR') for c in list(fdf.columns)]  