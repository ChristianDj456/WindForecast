{{ config(materialized='view') }} -- Se toman los nombres de cada region, junto a su media y mediana de la velociad
-- y direccion del viento por cada region de colombia junto a el numero de estaciones de cada una
SELECT DISTINCT Region,
  PERCENTILE_CONT(Velocidad, 0.5) OVER (PARTITION BY Region) AS MedianaVelocidad,
  AVG(Velocidad) OVER (PARTITION BY Region) AS MediaVelocidad,
  PERCENTILE_CONT(Direccion, 0.5) OVER (PARTITION BY Region) AS MedianaDireccion,
  AVG(Direccion) OVER (PARTITION BY Region) AS MediaDireccion,
  COUNT(DISTINCT Codigo) OVER (PARTITION BY Region) AS NumeroEstaciones
FROM
  {{ ref('WindsComplete') }}  
GROUP BY Region, Velocidad, Codigo, Direccion
ORDER BY Region