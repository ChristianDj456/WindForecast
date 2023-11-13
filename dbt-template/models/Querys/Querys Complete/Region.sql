{{ config(materialized='view') }}
SELECT DISTINCT Region,
  PERCENTILE_CONT(Velocidad, 0.5) OVER (PARTITION BY Region) AS MedianaVelocidad,
  AVG(Velocidad) OVER (PARTITION BY Region) AS MediaVelocidad,
  PERCENTILE_CONT(Direccion, 0.5) OVER (PARTITION BY Region) AS MedianaDireccion,
  AVG(Direccion) OVER (PARTITION BY Region) AS MediaDireccion,
  COUNT(Codigo) OVER (PARTITION BY Region) AS NumeroEstaciones
FROM
  {{ ref('WindsComplete') }}  
GROUP BY Region, Velocidad, Codigo, Direccion
ORDER BY Region