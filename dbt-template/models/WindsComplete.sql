{{ config(materialized='table') }}

SELECT Fecha, Codigo, CodigoSensor, Hora, PromedioVel AS Velocidad, ValorObservadoDireccion AS Direccion, NombreEstacion,
Departamento, Municipio, Region, ZonaHidrografica, Latitud, Longitud, Media, Mediana
FROM {{ref('UnionTablas')}}
ORDER BY
Fecha