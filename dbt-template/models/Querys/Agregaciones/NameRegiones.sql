{{ config(materialized='ephemeral')}}
WITH temporal AS (
  SELECT 
    FechaObservacion,
    CodigoEstacion AS Codigo,
    ValorObservado AS Viento,
    Departamento AS dep,
    Municipio,
    NombreEstacion,
    ZonaHidrografica,
    Latitud,
    Longitud
  FROM {{ ref('WindSpeed') }}
)
-- Se renombran los departamentos iguales, pero que tienen nombres distintos en la tabla de Velocidad del viento
SELECT 
  FechaObservacion AS FechaObservacion, 
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  FLOOR(EXTRACT(MINUTE FROM FechaObservacion)/10)*10 AS Minuto,
  * EXCEPT(FechaObservacion, dep, Latitud, Longitud),
  CASE
    WHEN dep IN ('SAN ANDRES PROVIDENCIA', 'ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA', 'ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA') THEN 'SAN ANDRES PROVIDENCIA'
    WHEN dep IN ('BOGOTA D.C.', 'BOGOTA') THEN 'BOGOTA D.C.'
    ELSE dep
  END AS Departamento,
  Latitud, 
  Longitud
FROM temporal