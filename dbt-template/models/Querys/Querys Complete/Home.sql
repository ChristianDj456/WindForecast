{{ config(materialized='view') }}
SELECT 
  AVG(Velocidad) AS media, -- Media total de la velocidad del viento
  APPROX_QUANTILES(Velocidad, 2)[OFFSET(1)] AS mediana, -- Mediana total de la velocidad del viento
  COUNT(DISTINCT Municipio) AS cantMunicipio, -- Numero de Municipios
  COUNT(DISTINCT Departamento) AS canDepartamento, -- Numero de Departamentos
  COUNT(DISTINCT Region) AS cantRegion, -- Numero de Regiones
  COUNT(DISTINCT Codigo) AS cantEstacion, -- Numero de Estaciones Registradas
  COUNT(*) AS cantRegistros -- Cantidad de Datos
FROM {{ ref('WindsComplete') }}