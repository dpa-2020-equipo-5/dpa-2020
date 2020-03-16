#Autor: Gerardo Mathus - @gemathus
#Autor: Karla Alfaro - @alpika
#oct 2019

import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
from pprint import pprint
from eda_gda import commons
from eda_gda.markdown_generator import MarkdownWriter

pd.set_option('mode.chained_assignment', None)
sns.set_style('darkgrid')

def is_outlier(points, thresh=3.5):
    if len(points.shape) == 1:
        points = points[:,None]
    median = np.median(points, axis=0)
    diff = np.sum((points - median)**2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.median(diff)

    modified_z_score = 0.6745 * diff / med_abs_deviation

    return modified_z_score > thresh

class DataProfile:
    def __init__(self, name, df, ignore_numeric=[], ignore_categoric=[], date_variables=[], force_categorics=[]):
        self.name = name
        self.df = df
        self.ignore_numeric = ignore_numeric
        self.ignore_categoric = ignore_categoric
        self.date_variables = date_variables
        self.force_categorics = force_categorics
        self.change_datatypes()

    def change_datatypes(self):
        for var in self.force_categorics:
            self.df[var] = self.df[var].astype('object')

    def get_all_variables(self):
        return list(self.df.columns)
    
    def get_numeric_variables(self, ignore=False):
        if ignore:
            return list(self.df.select_dtypes('number').drop(self.ignore_numeric, axis=1).columns)
        else:
            return list(self.df.select_dtypes('number').columns)

    def get_object_variables(self):
        object_vars = list(self.df.select_dtypes('object').columns)
        for var in object_vars:
            if var in self.date_variables:
                object_vars.remove(var)
        return object_vars

    def get_shape(self):
        return self.df.shape
    
    def get_unique_observations(self):
        return len(self.df.drop_duplicates())

    def get_summary(self):
        return self.df.describe()
    
    def get_unique_observations_per_variable(self):
        df_uniques =  self.df.select_dtypes('number').nunique().reset_index()
        df_uniques.drop(0, inplace=True)
        df_uniques.rename(columns = {'index':'variable', 0:'unique_values','0':'unique_values'}, inplace=True)
        return df_uniques
    
    def get_top_5_repeated_values(self, series):
        df_top5 = self.df[series].value_counts().head(5).reset_index()
        df_top5.columns = ['variable_value', 'absolute_frequency']
        #df_top5.rename(columns = {'index':'variable_value', 0:'absolute_frequency','0':'absolute_frequency'}, inplace=True)
        return df_top5

    def get_mode(self, series):
        amount_of_modes = 3
        while len(self.df[series].value_counts().head(amount_of_modes)) < amount_of_modes:
            amount_of_modes -= 1

        modes = self.df[series].value_counts().head(amount_of_modes).reset_index()
        modes.rename(columns = {'index':'variable_value', series:'absolute_frequency'}, inplace=True)
        dff = pd.DataFrame({
            'variable': [series] * amount_of_modes,
            'value': list(modes.variable_value.values),
            'frequency': list(modes.absolute_frequency.values),
            'repetition_rate': [0] * amount_of_modes
        })
        dff.repetition_rate = (dff['frequency'] / len(self.df[series].dropna())).round(decimals=2)
        return dff
    
    def get_missing_values_per_column(self):
        dff = self.df.isna().sum().reset_index()
        dff.rename( columns={'index':'variable', 0:'nas'}, inplace=True)
        dff.drop(0, inplace=True)
        dff['proportion'] = dff.nas / len(self.df) 
        dff['percentage'] =  dff.proportion * 100
        dff.percentage = dff.percentage.round(decimals=2)
        dff.drop(['proportion'],axis=1, inplace=True)
        return dff

    def detect_rounded_up_values(self):
        dff = pd.DataFrame({
            'variable':[],
            'value': [],
            'frequency': [],
            'repetition_rate': []
        })
        for series in self.df.select_dtypes('number'):
            dff = dff.append(self.get_mode(series))
        return dff

    def plot_continous_variables_distributions(self):
        plots = []
        vars = self.get_numeric_variables(ignore=True)
        for var in vars:
            if len(self.df[var].unique()) == 1:
                print("-> No se graficará la distribución de la variable `{}` porque tiene un solo valor único".format(var))
                continue
            print("\t-> Creando gráfica para `{}`".format(var))
            fig, ax = plt.subplots(figsize=(14,8))
            sns.distplot(self.df[var].dropna(), kde=False, ax=ax)
            save_path = 'docs/img/distplot_raw_{}.png'.format(var)
            fig.savefig(save_path,bbox_inches='tight')
            plots.append([var, save_path])
            plt.clf()

            fig, ax = plt.subplots(figsize=(14,8))
            sns.countplot(self.df[var].dropna(), ax=ax)
            save_path = 'docs/img/countplot_raw_{}.png'.format(var)
            fig.savefig(save_path,bbox_inches='tight')
            plots.append([var, save_path])
            plt.clf()

            points_no_outliers = self.df[var].dropna()
            points_no_outliers = points_no_outliers[~is_outlier(points_no_outliers, thresh=2)]
            fig, ax = plt.subplots(figsize=(14,8))
            sns.distplot(points_no_outliers, kde=False, ax=ax) 
            save_path = 'docs/img/distplot_no_outliers_{}.png'.format(var)
            fig.savefig(save_path,bbox_inches='tight')
            plots.append([var + ' sin outliers', save_path])
            plt.clf()

            fig, ax = plt.subplots(figsize=(14,8))
            sns.countplot(points_no_outliers, ax=ax) 
            save_path = 'docs/img/countplot_no_outliers_{}.png'.format(var)
            fig.savefig(save_path,bbox_inches='tight')
            plots.append([var + ' sin outliers', save_path])
            plt.clf()

            plt.close('all')

        return plots

    def plot_categoric_variables(self):
        plots = []
        vars = self.get_object_variables()
        for var in vars:
            if len(self.df[var].unique()) == 1:
                print("\t-> No se graficará la distribución de la variable `{}` porque tiene un solo valor único".format(var))
                continue
            if len(self.df[var].unique()) > 55:
                print("\t-> No se grafícara la distribución de la variable `{}` porque tiene {} clases. Una gráfica de frecuencias con este número de clases resultará en una gráfica poco legible.\n".format(var, len(self.df[var].unique())))
                continue
            
            print("\t-> Creando gráfica para `{}`".format(var))
            save_path = 'docs/img/countplot_{}.png'.format(var)
            p = sns.countplot(data=self.df, order = self.df[var].value_counts().index, x=var)
            plt.xticks(rotation=90)
            f = p.get_figure()
            f.savefig(save_path,bbox_inches='tight')
            plt.clf()
            plots.append([var, save_path])
        plt.close('all')
        return plots
    
    def get_missing_values_categoric(self):
        dff = self.df.select_dtypes('object').isna().sum().reset_index()
        dff.rename( columns={'index':'variable', 0:'nas'}, inplace=True)
        dff.drop(0, inplace=True)
        dff['proportion'] = dff.nas / len(self.df) 
        dff['percentage'] =  dff.proportion * 100
        dff.percentage = dff.percentage.round(decimals=2)
        dff.drop(['proportion'],axis=1, inplace=True)
        return dff

    def plot_missing_values_heatmap(self, dtype='object'):
        plots = []
        save_path = 'docs/img/{}_null_values_heatmap.png'.format(dtype)
        p = sns.heatmap(self.df.select_dtypes(dtype).isnull(), yticklabels=False, cbar=False, cmap='flag')
        f = p.get_figure()
        f.savefig(save_path,bbox_inches='tight')
        plt.clf()
        plots.append(['{} variables with null vals'.format(dtype), save_path])
        plt.close('all')
        return plots

    def centralization_summary(self):
        print("\t-> Resumen de centralización")
        dff = self.df.select_dtypes('number').describe().drop(self.ignore_numeric, axis=1)
        data = [dff.loc['mean'].values, dff.loc['50%'].values,[0]*len(dff.columns)] 
        cols = dff.columns
        indexes = ['media', 'mediana', 'moda']
        centralizacion = pd.DataFrame(data, columns = cols, index = indexes)
        for col in centralizacion:
            centralizacion[col].loc['moda'] = self.df[col].value_counts().head(1).index[0]
        return centralizacion

    def shape_summary(self):
        print("\t-> Resumen de forma")
        dff = self.df.describe().drop(self.ignore_numeric, axis=1)
        cols = dff.columns
        data = [[0] * len(cols), [0] * len(cols)]
        indexes = ['asimetria', 'kurtosis']
        forma = pd.DataFrame(data, columns = cols, index = indexes) 
        forma.loc['asimetria'] = self.df.select_dtypes('number').skew(axis = 0, skipna = True)
        forma.loc['kurtosis'] = self.df.select_dtypes('number').kurtosis(axis = 0, skipna = True)
        return forma

    def position_summary(self):
        print("\t-> Resumen de posición")
        dff = self.df.select_dtypes('number').describe().drop(self.ignore_numeric, axis=1)
        data = [dff.loc['max'].values, dff.loc['75%'].values, dff.loc['50%'].values,dff.loc['25%'].values, dff.loc['min'].values] 
        cols = dff.columns
        indexes = ['max', '75%', '50%', '25%', 'min']
        posicion = pd.DataFrame(data, columns = cols, index = indexes) 
        return posicion

    def dispersion_summary(self):
        print("\t-> Resumen de dispersión")
        dff = self.df.select_dtypes('number').describe().drop(self.ignore_numeric, axis=1)
        cols = dff.columns
        data = [dff.loc['std'].values, [0] * len(cols), [0] * len(cols)]
        indexes = ['desv. est.', 'varianza', 'rango']
        dispersion = pd.DataFrame(data, columns = cols, index = indexes) 
        dispersion.loc['varianza'] = dispersion.loc['desv. est.'] ** 2
        dispersion.loc['rango'] = dff.loc['max'] - dff.loc['min']
        return dispersion

    def uniques_and_missing_summary(self):
        print("\t-> Resumen de valores únicos y faltantes")
        dff = self.df.describe().drop(self.ignore_numeric, axis=1)
        cols = dff.columns
        data = [[0] * len(cols), [0] * len(cols), [0] * len(cols),[0] * len(cols), [0] * len(cols)]
        indexes = ['Número de observaciones','Valores únicos', '% Únicos', 'Valores faltantes', '% Faltantes']
        unicos_faltantes = pd.DataFrame(data, columns = cols, index = indexes) 
        unicos_faltantes.loc['Número de observaciones'] = dff.loc['count']
        unicos_faltantes.loc['Valores únicos'] = self.df.select_dtypes('number').nunique()
        unicos_faltantes.loc['% Únicos'] = (unicos_faltantes.loc['Valores únicos'] / dff.loc['count'] * 100).round(decimals=2) 
        unicos_faltantes.loc['Valores faltantes'] = self.df.select_dtypes('number').isna().sum()
        unicos_faltantes.loc['% Faltantes'] = ((self.df.select_dtypes('number').isna().sum()  / len(self.df.select_dtypes('number'))) * 100).round(decimals=2)
        return unicos_faltantes

    def repeated_values_summary(self):
        print("\t-> Resumen de valores repetidos")
        dff = self.df.describe().drop(self.ignore_numeric, axis=1)
        cols = dff.columns
        data = [
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols)
        ]
        
        indexes = ['valor 1', 'frecuencia valor 1', 'valor 2', 'frecuencia valor 2', 'valor 3', 'frecuencia valor 3', 'valor 4', 'frecuencia valor 4', 'valor 5', 'frecuencia valor 5']
        repetidos = pd.DataFrame(data, columns = cols, index = indexes) 

        for col in self.df.select_dtypes('number').drop(self.ignore_numeric, axis=1):
            top_5 = self.df[col].value_counts().head(5)
            for i in range(len(top_5)):
                repetidos[col].loc['valor ' + str(i + 1)] = top_5.index[i]
                repetidos[col].loc['frecuencia valor '+ str(i + 1)] = top_5.values[i]
                
        return repetidos

    def repeated_categoric_values_summary(self):
        print("\t-> Resumen de valores categóricos repetidos")
        dff = self.df.select_dtypes('object')
        cols = dff.columns
        data = [
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols)
        ]
        
        indexes = ['valor 1', 'frecuencia valor 1', 'valor 2', 'frecuencia valor 2', 'valor 3', 'frecuencia valor 3', 'valor 4', 'frecuencia valor 4', 'valor 5', 'frecuencia valor 5']
        repetidos = pd.DataFrame(data, columns = cols, index = indexes) 

        for col in self.df.select_dtypes('object'):
            top_5 = self.df[col].value_counts().head(5).copy()
            for i in range(len(top_5)):
                repetidos[col].loc['valor ' + str(i + 1)] = top_5.index[i]
                repetidos[col].loc['frecuencia valor '+ str(i + 1)] = top_5.values[i]

        return repetidos

    def categoric_observations_summary(self):
        print("\t-> Resumen de observaciones de variables categóricas")
        dff = self.df.select_dtypes('object')
        cols = dff.columns
        data = [
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols),
            [0] * len(cols), [0] * len(cols)
        ]

        indexes = ['Número de observaciones','Número de categorias', 'Valores únicos', 'Valores únicos (%)', 'Valores faltantes', 'Valores faltantes (%)']
        observations = pd.DataFrame(data, columns = cols, index = indexes)
        observations.loc['Número de observaciones'] = dff.count()
        observations.loc['Número de categorias'] = dff.nunique()
        observations.loc['Valores únicos'] = dff.nunique()
        observations.loc['Valores únicos (%)'] = ((observations.loc['Valores únicos'] / observations.loc['Número de categorias']) * 100).round(decimals=2)
        observations.loc['Valores faltantes'] =  dff.isna().sum()
        observations.loc['Valores faltantes (%)'] = ((observations.loc['Valores faltantes'] / len(dff)) * 100).round(decimals=2)
        return observations
    
    def categoric_variables_values(self):
        print("\t-> Resumen de valores de variables categóricas")
        dff = self.df.select_dtypes('object')
        cols = dff.columns
        data = [
            [0] * len(cols)
        ]
        indexes = ['Categorías']
        categories = pd.DataFrame(data, columns = cols, index = indexes)
        try:
            for col in dff:
                if len(dff[col].unique()) > 50:
                    categories[col].loc['Categorías'] = str(len(dff[col].unique())) + ' categorías'
                else:
                    categories[col].loc['Categorías'] = ', '.join(list(dff[col].unique()))
            return categories
        except:
            return pd.DataFrame(data, columns = cols, index = indexes)
        
    
    def plot_correlation_matrix(self):
        plots = []
        save_path = 'docs/img/correlation_matrix.png'
        dff = self.df[self.get_numeric_variables(ignore=True)]
        fig, ax = plt.subplots(figsize=(14,8))
        corr = dff.corr()

        # Generate a mask for the upper triangle
        mask = np.zeros_like(corr, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True

        sns.heatmap(corr, mask=mask, cmap='YlGnBu', annot=True, ax=ax)
        fig.savefig(save_path,bbox_inches='tight')
        plt.clf()
        plots.append(['Matriz de correlación', save_path])
        plt.close('all')
        return plots

    def get_profile_as_md(self):
        markdown = MarkdownWriter(self.name)
        markdown.append(self.name, "h1")
        markdown.append("Data Profiling", "h2")
        markdown.append(date.today().strftime("%d/%m/%Y"), "h5")
        markdown.append("", "hr")

        markdown.append("1. Variables", "h2")
        temp = self.get_all_variables() 
        markdown.append(str(len(temp)) + " variables")
        markdown.append(temp, "ul")
        
        markdown.append("1.1. Variables numéricas", "h3")
        temp = self.get_numeric_variables() 
        markdown.append(str(len(temp)) + " variables numéricas")
        markdown.append(temp, "ul")

        markdown.append("1.2. Variables categóricas", "h3")
        temp = self.get_object_variables()
        for date_var in self.date_variables:
            if date_var in temp:
                temp.remove(date_var)
        markdown.append(str(len(temp)) + " variables categóricas")
        markdown.append(temp, "ul")

        markdown.append("1.2. Variables de fecha", "h3")
        markdown.append(str(len(self.date_variables)) + " variables de fecha")
        markdown.append(self.date_variables, "ul")

        

        markdown.append("1.3. Dimensión", "h3")
        shape = self.get_shape()
        markdown.append("Filas: {}, Columnas: {}".format(shape[0], shape[1]))
        
        #unique observations
        markdown.append("1.4. Observaciones únicas", "h3")
        markdown.append(str(self.get_unique_observations()))

        markdown.append("1.5. Resumen de variables", "h3")
        print("\t-> Data profiling de variables numéricas")
        markdown.append("1.5.1. Variables numéricas", "h4")

        #valores únicos y faltantes
        markdown.append("Observaciones", 'h4')
        markdown.append(self.uniques_and_missing_summary(), 'tblwi')

        # centralización
        markdown.append("Centralización", "h4")
        markdown.append(self.centralization_summary(), 'tblwi')

        #posición
        markdown.append("Posición", "h4")
        markdown.append(self.position_summary(), 'tblwi')

        #dispersión
        markdown.append("Dispersión", "h4")
        markdown.append(self.dispersion_summary(), 'tblwi')

        #forma
        markdown.append("Forma", "h4")
        markdown.append(self.shape_summary(), 'tblwi')


        #valoes repetidos
        markdown.append("Valores repetidos", 'h4')
        markdown.append(self.repeated_values_summary(), 'tblwi')

        # ¿Hay redondeos?      
        markdown.append("1.5.1.2. Posibles redondeos", "h5")
        markdown.append(self.detect_rounded_up_values(), "tbl") #tblwi = table 

        #gráfica de valores faltantes
        markdown.append("1.5.5.3. Gráfica de valores faltantes", "h5")
        plot = self.plot_missing_values_heatmap(dtype='number')[0]
        markdown.append(plot, "img")

        #distribuciones
        markdown.append("1.5.4. Distrubuciones", "h5")
        numeric_plots = self.plot_continous_variables_distributions()
        for ind, plot in enumerate(numeric_plots):
            markdown.append("1.5.4.{}. {}".format(ind + 1, plot[0]), "h5")
            markdown.append(plot, "img")
        
        markdown.append("1.5.5. Matriz de correlación", "h5")
        plot = self.plot_correlation_matrix()[0]
        markdown.append(plot, "img")

        print("\t-> Data profiling de variables categóricas")
        markdown.append("1.5.2. Resumen de variables categóricas", "h4")

        #Observaciones
        markdown.append("Observaciones", "h4")
        markdown.append(self.categoric_observations_summary(), 'tblwi')

        #categorías
        markdown.append("Categorías", "h4")
        markdown.append(self.categoric_variables_values(), 'tblwi')

        #Moda
        #valoes repetidos
        markdown.append("Valores repetidos", 'h4')
        markdown.append(self.repeated_categoric_values_summary(), 'tblwi')

        #Histograma
        markdown.append("1.5.2.2. Gráficas de frecuencias", "h4")
        categoric_plots = self.plot_categoric_variables()
        for ind,plot in enumerate(categoric_plots):
            markdown.append("1.5.2.2.{}. {}".format(ind + 1, plot[0]), "h5")
            markdown.append(plot, "img")

        plt.close('all')
        markdown.save()
        return markdown.file_name, markdown.content