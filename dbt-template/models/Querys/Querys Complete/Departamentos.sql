{{ config(materialized='view') }}-- Se toman los nombres de cada departamento, junto a su media y mediana de la velociad
-- y direccion del viento por cada region de colombia junto a el numero de estaciones de cada una
  SELECT Departamento,
  COUNT(DISTINCT Codigo) AS CantidadEstaciones,
  AVG(Velocidad) AS MediaVelocidad,
  APPROX_QUANTILES(Velocidad, 2)[OFFSET(1)] AS MedianaVelocidad,
  AVG(Direccion) AS MediaDireccion,
  APPROX_QUANTILES(Direccion, 2)[OFFSET(1)] AS MedianaDireccion
FROM
  {{ ref('WindsComplete') }}
GROUP BY
Departamento