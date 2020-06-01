# Predictions API

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_predictions**](PredictionsApi.md#get_predictions) | **GET** /prediction | Obtener la lista de centros a verificar más reciente.
[**get_predictions_by_date**](PredictionsApi.md#get_predictions_by_date) | **GET** /prediction/{date} | Obtener la lista de centros a verificar para una fecha en específico.

# **get_predictions**
> Prediction get_predictions()

Obtiene la lista de centros a verificar más reciente.

### Response

[**Prediction**](PredictionsApi.md#Prediction)

# **get_predictions_by_date**
> Prediction get_predictions_by_date(date)

Obtiene una lista de centros para un año y mes en específico.

### Parameters

Name | Type | Description  
------------- | ------------- | -------------
 **date** | **string**| Fecha en formato yyyy-MM-dd

### Response

[**Prediction**](PredictionsApi.md#Prediction)

# Prediction

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**date** | **string** |  Fecha de la predicción| Formato: 'yyyy-MM-dd' 
**centers** | [**list[Inspection]**](PredictionsApi.md#Inspection) |  Lista de centros a inspeccionar| 


# Inspection

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**center_id** | **str** | Id del centro a inspeccionar | 
**ranking** | **int** | Número que indica el orden de revisión sugerido | Entre más alto el riesgo, mayor el ranking 
