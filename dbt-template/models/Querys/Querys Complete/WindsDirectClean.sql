{{ config(materialized='ephemeral')}}
WITH tabla AS (
  SELECT 
    CodigoEstacion AS Codigo, 
    FechaObservacion AS Fecha,
    EXTRACT(HOUR FROM FechaObservacion) AS Hora,
    ValorObservado AS Direccion,
    Municipio,
    Departamento,
    Latitud,
    Longitud,
  FROM {{ ref('WindsDirect') }} 
), limpiando_caracteres AS (
  SELECT 
    TRANSLATE(UPPER(Departamento), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') AS Departamento,
    TRANSLATE(UPPER(Municipio), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') AS Municipio,
    DATE(Fecha) AS Fecha,
    * EXCEPT (Departamento, Municipio, Fecha)
  FROM tabla
), nombres_departamentos AS (
  SELECT 
    CASE
      WHEN Departamento LIKE '%SAN ANDRES%' THEN 'SAN ANDRES PROVIDENCIA'
      WHEN Departamento LIKE '%BOGOTA%' THEN 'BOGOTA D.C.'
      ELSE Departamento
    END AS Departamento,
    * EXCEPT(Departamento)
  FROM limpiando_caracteres
), regiones AS (
  SELECT 
    CASE
      WHEN Departamento IN ('AMAZONAS', 'CAQUETA', 'GUAINIA', 'GUAVIARE', 'PUTUMAYO') THEN 'AMAZONICA'
      WHEN Departamento IN ('ANTIOQUIA', 'BOGOTA D.C.', 'BOYACA', 'CALDAS', 'CUNDINAMARCA', 'HUILA', 'NORTE DE SANTANDER', 'QUINDIO', 'RISARALDA', 'SANTANDER', 'TOLIMA') THEN 'ANDINA'
      WHEN Departamento IN ( 'ATLANTICO', 'BOLIVAR', 'CESAR', 'CORDOBA', 'LA GUAJIRA', 'MAGDALENA', 'SUCRE') THEN 'CARIBE'
      WHEN Departamento IN ('CAUCA', 'CHOCO', 'NARINO', 'NARIÑO', 'VALLE DEL CAUCA') THEN 'PACIFICO'
      WHEN Departamento IN ('ARAUCA', 'CASANARE', 'META', 'VICHADA') THEN 'ORINOQUIA'
      WHEN Departamento IN ('SAN ANDRES PROVIDENCIA') THEN 'INSULAR'
      ELSE NULL -- Casos no mapeados
    END AS Region,
    *
  FROM nombres_departamentos
), promedio_hora AS (
  SELECT 
    Codigo,
    Fecha, 
    Hora, 
    AVG(Latitud) AS Latitud,
    AVG(Longitud) AS Longitud,
    Region, 
    Departamento, 
    Municipio,
    APPROX_QUANTILES(Direccion, 2)[OFFSET(2)] AS Direccion
  FROM regiones
  GROUP BY Codigo, Fecha, Hora, Region, Departamento, Municipio
), datos_atipicos AS (
  SELECT *
  FROM promedio_hora
  WHERE Municipio NOT IN ("BELLO", "PUERTO GAITAN", "AYAPEL", "PEREIRA", "PURACE (COCONUCO)") AND Direccion > 0
)

SELECT *
FROM promedio_hora
ORDER BY Fecha, Hora, Region