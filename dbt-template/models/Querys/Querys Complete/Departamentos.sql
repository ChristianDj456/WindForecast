{{ config(materialized='view') }}
SELECT DISTINCT Departamento,
  COUNT(Codigo) AS Cantidad_Estaciones,
  AVG(Velocidad) AS Media_Velocidad,
  APPROX_QUANTILES(Velocidad, 2)[OFFSET(1)] AS Mediana_Velocidad
FROM
  {{ ref('WindsComplete') }}
GROUP BY
  Codigo, Departamento
  ORDER BY
  Departamento