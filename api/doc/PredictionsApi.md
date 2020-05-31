# PredictionsApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_predictions_by_date**](PredictionsApi.md#get_predictions_by_date) | **GET** /prediction/{date} | Obtener una lista de centros para un año y mes en específico.

# **get_predictions_by_date**
> Prediction get_predictions_by_date(date)

Obtiene una lista de centros para un año y mes en específico.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **date** | **string**| Fecha en formato yyyy-MM | El mes debe ser válido (1-12)

### Response

[**Prediction**](Prediction.md)


# Prediction

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**date** | **string** |  Fecha de la predicción| Formato: 'yyyy-MM' 
**centers** | [**list[Inspection]**](Inspection.md) |  Lista de centros a inspeccionar| 


# Inspection

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**center_id** | **str** | Id del centro a inspeccionar | 
**ranking** | **int** | Número que indica el orden de revisión sugerido | Entre más alto el riesgo, mayor el ranking 
