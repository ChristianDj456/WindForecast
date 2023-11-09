SELECT DISTINCT
  Codigo,
  CodigoSensor,
  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0)) AS Fecha,
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  AVG(DirViento) OVER(PARTITION BY  DATETIME(DATE(FechaObservacion), TIME(EXTRACT(HOUR FROM FechaObservacion),0,0))) AS PromedioDir,
  NombreEstacion,
  Departamento,
  Municipio,
  ZonaHidrografica,
  Latitud,
  Longitud,
  DescripcionSensor,
  Region,
FROM
  {{ ref('RegionesDir') }}
ORDER BY
  Codigo,
  Fecha,
  Hora