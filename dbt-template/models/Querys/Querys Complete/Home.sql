{{ config(materialized='view') }}
SELECT 
  AVG(Velocidad) AS media, 
  APPROX_QUANTILES(Velocidad, 2)[OFFSET(1)] AS mediana, 
  COUNT(DISTINCT Municipio) AS cantMunicipio,
  COUNT(DISTINCT Departamento) AS canDepartamento,
  COUNT(DISTINCT Region) AS cantRegion,
  COUNT(DISTINCT Codigo) AS cantEstacion,
  COUNT(*) AS cantRegistros
FROM {{ ref('WindsComplete') }}