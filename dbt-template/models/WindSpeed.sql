{{ config(materialized='ephemeral')}}
-- Se extrae la información de la tabla Externa subida a BigQuery, y se hace una limpieza de las
-- letras con acentos y diacríticos
SELECT CodigoEstacion, CodigoSensor, FechaObservacion, ValorObservado, NombreEstacion, TRANSLATE(UPPER(Departamento), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') AS Departamento, 
TRANSLATE(UPPER(Municipio), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') AS Municipio, ZonaHidrografica, Latitud, Longitud, DescripcionSensor, UnidadMedida
FROM {{ source('WindsForecast', 'WindSpeed_EXT') }}