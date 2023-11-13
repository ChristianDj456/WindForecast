{{ config(materialized='view') }}
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