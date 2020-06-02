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


programtype = pd.get_dummies(centros).idxmax(1)

categorias = ["borough_bronx","borough_brooklyn","borough_manhattan", "borough_queens", "borough_staten_island"]

borough = pd.get_dummies(centros[categorias]).idxmax(1)

ambas = pd.concat([borough, programtype], axis=1,)

ambas = ambas.rename(columns={0:'borough', 1:'programtype'})

centros = pd.concat([centros, ambas], axis=1)

tabla = pd.merge(res, centros, left_on='center', right_on='center_id')

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

bdf = b.get_disparity_predefined_groups(xtab, original_df=tabla, ref_groups_dict={'borough':'borough_brooklyn', 'programtype':'programtype_preschool'}, alpha=0.05, mask_significance=True)

hbdf = b.get_disparity_predefined_groups(xtab, original_df=tabla,
                                         ref_groups_dict={'borough':'borough_brooklyn', 'programtype':'programtype_preschool'},
                                         alpha=0.05,
                                         mask_significance=False)


majority_bdf = b.get_disparity_major_group(xtab, original_df=tabla, mask_significance=True)

f = Fairness()

fdf = f.get_group_value_fairness(bdf)

fg = aqp.plot_fairness_group_all(fdf, ncols=5, metrics = "all")

a_tm = aqp.plot_fairness_disparity_all(fdf, attributes=['borough'], metrics='all',
                                       significance_alpha=0.05)


