{{ config(materialized='ephemeral')}}

SELECT Fecha, Codigo, Region, Departamento, Municipio, PromedioLa AS Latitud, PromedioLo AS Longitud, PromedioVel AS Velocidad
FROM
{{ ref('PromByHours') }}
ORDER BY
Fecha