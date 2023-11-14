{{ config(materialized='ephemeral')}}
WITH temporal AS (
  SELECT 
    FechaObservacion,
    Hora,
    Minuto,
    Codigo,
    DirViento,
    Departamento,
    Municipio,
    NombreEstacion,
    ZonaHidrografica,
    Latitud,
    Longitud
  FROM {{ref('NameRegionesDir')}}
)
-- Se crea la columna Region en la tabla de Dirección del viento, agrupando sus respectivos Departamentos
SELECT 
  FechaObservacion AS FechaObservacion, 
  * EXCEPT(FechaObservacion, Latitud, Longitud),
  CASE
    WHEN Departamento IN ('AMAZONAS', 'CAQUETA', 'GUAINIA', 'GUAVIARE', 'PUTUMAYO') THEN 'AMAZONICA'
    WHEN Departamento IN ('ANTIOQUIA', 'BOGOTA D.C.', 'BOYACA', 'CALDAS', 'CUNDINAMARCA', 'HUILA', 'NORTE DE SANTANDER', 'QUINDIO', 'RISARALDA', 'SANTANDER', 'TOLIMA') THEN 'ANDINA'
    WHEN Departamento IN ( 'ATLANTICO', 'BOLIVAR', 'CESAR', 'CORDOBA', 'LA GUAJIRA', 'MAGDALENA', 'SUCRE') THEN 'CARIBE'
    WHEN Departamento IN ('CAUCA', 'CHOCO', 'NARINO', 'NARIÑO', 'VALLE DEL CAUCA') THEN 'PACIFICO'
    WHEN Departamento IN ('ARAUCA', 'CASANARE', 'META', 'VICHADA') THEN 'ORINOQUIA'
    WHEN Departamento IN ('SAN ANDRES PROVIDENCIA') THEN 'INSULAR'
    ELSE NULL -- Casos no mapeados
  END AS Region,
  Latitud, 
  Longitud
FROM temporal