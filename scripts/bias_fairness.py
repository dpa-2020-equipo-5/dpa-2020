# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 21:25:06 2020

@author: Elizabeth
"""

################### BIAS AND FAIRNESS ####################

import pandas as pd
import seaborn as sns
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from aequitas.plotting import Plot
import seaborn as sns; sns.set()
from numpy import arange

import warnings; warnings.simplefilter('ignore')

%matplotlib inline

prds = rforest.predict(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))

probas = rforest.predict_proba(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))

res = pd.DataFrame({
    "center":tabla_5.index,
    "etiqueta":prds,
    "proba_0":probas[:,0],
    "proba_1":probas[:,1]
})

res.loc[res['proba_0'] > res['proba_1'], 'score'] = res['proba_0']
res.loc[res['proba_0'] < res['proba_1'], 'score'] = res['proba_1']

tabla_2 = df.loc[:, ['centername', 'legalname', 'building', 'street', 'borough', 'zipcode', 'phone', 'permitnumber', 
                     'permitexp', 'status', 'agerange', 'maximumcapacity', 'dc_id', 'programtype', 'facilitytype', 
                     'childcaretype', 'bin', 'url', 'datepermitted', 'actual', 'violationratepercent', 'violationavgratepercent', 
                     'totaleducationalworkers', 'averagetotaleducationalworkers', 'publichealthhazardviolationrate', 
                     'averagepublichealthhazardiolationrate', 'criticalviolationrate', 'avgcriticalviolationrate']]

tabla_2.info()

tabla_2 = tabla_2.drop_duplicates()

tabla_2.shape

tabla = pd.merge(res, tabla_2, left_on='center', right_on='dc_id')

tabla = tabla.loc[:, ['center', 'etiqueta', 'score', 'borough', 'programtype']]

tabla =  tabla.rename(columns = {'etiqueta':'label_value'})

tabla = tabla.set_index(['center'])


######¿Qué sesgo existe en el modelo?########
              
g = Group()

xtab, _ = g.get_crosstabs(tabla)

absolute_metrics = g.list_absolute_metrics(xtab)

xtab[[col for col in xtab.columns if col not in absolute_metrics]]

aqp = Plot()

a = aqp.plot_group_metric_all(xtab, ncols=3)

######¿Cuál es el nivel de disparidad qué existe entre los grupos de población (categorías de referencia)########

b = Bias()

bdf = b.get_disparity_predefined_groups(xtab, original_df=tabla, ref_groups_dict={'borough':'brooklyn', 'programtype':'preschool'}, alpha=0.05, mask_significance=True)

hbdf = b.get_disparity_predefined_groups(xtab, original_df=tabla,
                                         ref_groups_dict={'borough':'brooklyn', 'programtype':'preschool'},
                                         alpha=0.05,
                                         mask_significance=False)

hbdf[['attribute_name', 'attribute_value'] +  calculated_disparities + disparity_significance]

majority_bdf = b.get_disparity_major_group(xtab, original_df=tabla, mask_significance=True)

majority_bdf[['attribute_name', 'attribute_value'] +  calculated_disparities + disparity_significance]

f = Fairness()

fdf = f.get_group_value_fairness(bdf)

parity_detrminations = f.list_parities(fdf)

fdf[['attribute_name', 'attribute_value'] + absolute_metrics + calculated_disparities + parity_detrminations]

gaf = f.get_group_attribute_fairness(fdf)

gaf

fg = aqp.plot_fairness_group_all(fdf, ncols=5, metrics = "all")

a_tm = aqp.plot_fairness_disparity_all(fdf, attributes=['borough'], metrics='all',
                                       significance_alpha=0.05)


