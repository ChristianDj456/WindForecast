{{ config(materialized='ephemeral')}}
-- Se extrae la información de la tabla Externa subida a BigQuery, y se hace una limpieza de las
-- letras con acentos y diacríticos
SELECT *
FROM {{ source('WindsForecast', 'WindSpeed_EXT') }}