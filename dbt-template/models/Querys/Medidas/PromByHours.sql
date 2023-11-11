{{ config(materialized='table') }}
SELECT DISTINCT
  Codigo,
  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0)) AS Fecha,
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  AVG(Viento) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0)), Codigo) AS PromedioVel,
  NombreEstacion,
  Departamento,
  Municipio,
  ZonaHidrografica,
  AVG(Latitud) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioLa,
  AVG(Longitud) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioLo,
  Region,
FROM
  {{ ref('Regiones') }}
ORDER BY
  Codigo,
  Fecha,
  Hora