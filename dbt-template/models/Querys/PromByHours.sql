{{ config(materialized='table') }}

SELECT DISTINCT
  Codigo,
  CodigoSensor,
  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0)) AS Fecha,
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  AVG(Viento) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioVel,
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
ORDER BY
  Codigo,
  Fecha,
  Hora