{{ config(materialized='ephemeral')}}
SELECT CodigoEstacion, CodigoSensor, FechaObservacion, ValorObservado, NombreEstacion, TRANSLATE(UPPER(Departamento), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') AS Departamento, 
TRANSLATE(UPPER(Municipio), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') AS Municipio, ZonaHidrografica, Latitud, Longitud, DescripcionSensor, UnidadMedida
FROM {{ source('WindsForecast', 'WindSpeed_EXT') }}