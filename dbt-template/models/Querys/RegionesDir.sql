WITH temporal AS (
  SELECT 
    FechaObservacion,
    CodigoEstacion AS Codigo,
    ValorObservado AS DirViento,
    UPPER(Departamento) AS Departamento,
    UPPER(Municipio) AS Municipio,
    NombreEstacion,
    ZonaHidrografica,
    Latitud,
    Longitud
  FROM {{ ref('WindsDirect') }}
)

SELECT 
  FechaObservacion AS FechaObservacion, 
  EXTRACT(HOUR FROM FechaObservacion) AS Hora,
  FLOOR(EXTRACT(MINUTE FROM FechaObservacion)/10)*10 AS Minuto,
  * EXCEPT(FechaObservacion, Latitud, Longitud),
  CASE
    WHEN Departamento IN ('AMAZONAS', 'CAQUETA', 'GUAINIA', 'GUAVIARE', 'PUTUMAYO') THEN 'AMAZONICA'
    WHEN Departamento IN ('ANTIOQUIA', 'BOGOTA', 'BOGOTA D.C.', 'BOYACA', 'CALDAS', 'CUNDINAMARCA', 'HUILA', 'NORTE DE SANTANDER', 'QUINDIO', 'RISARALDA', 'SANTANDER', 'TOLIMA') THEN 'ANDINA'
    WHEN Departamento IN ('ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA', 'ATLANTICO', 'BOLIVAR', 'CESAR', 'CORDOBA', 'LA GUAJIRA', 'MAGDALENA', 'SAN ANDRES PROVIDENCIA', 'SUCRE') THEN 'CARIBE'
    WHEN Departamento IN ('CAUCA', 'CHOCO', 'NARINO', 'NARIÑO', 'VALLE DEL CAUCA') THEN 'PACIFICO'
    WHEN Departamento IN ('ARAUCA', 'CASANARE', 'META', 'VICHADA') THEN 'ORINOQUIA'
    ELSE NULL -- Casos no mapeados
  END AS Region,
  Latitud, 
  Longitud
FROM temporal