{{ config(materialized='table') }}

SELECT
  Codigo,
  CodigoSensor,
  DATE(FechaObservacion) AS Fecha,
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  AVG(Viento) AS PromedioValorObservado,
  NombreEstacion,
  Departamento,
  Municipio,
  ZonaHidrografica,
  Latitud,
  Longitud,
  DescripcionSensor,
  Region,
FROM
  {{ ref('Regiones') }}
GROUP BY
  Codigo,
  CodigoSensor,
  DATE(FechaObservacion),
  EXTRACT(HOUR FROM FechaObservacion),
  NombreEstacion,
  Departamento,
  Municipio,
  ZonaHidrografica,
  Latitud,
  Longitud,
  DescripcionSensor,
  Region
