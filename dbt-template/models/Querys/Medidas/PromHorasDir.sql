{{ config(materialized='table') }}
SELECT DISTINCT
  Codigo,
  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0)) AS Fecha,
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  AVG(DirViento) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioDir,
  NombreEstacion,
  Departamento,
  Municipio,
  ZonaHidrografica,
  AVG(Latitud) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioLaDir,
  AVG(Longitud) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioLoDir,
  Region,
FROM
  {{ ref('RegionesDir') }}
ORDER BY
  Codigo,
  Fecha,
  Hora