{{ config(materialized='view') }}
SELECT DISTINCT Region,
  PERCENTILE_CONT(Velocidad, 0.5) OVER (PARTITION BY Region) AS MedianaPorRegion,
  AVG(Velocidad) OVER (PARTITION BY Region) AS MediaPorRegion,
  COUNT(Codigo) OVER (PARTITION BY Region) AS NumeroEstaciones
FROM
  {{ ref('WindsComplete') }} 
GROUP BY Region, Velocidad, Codigo
ORDER BY Region