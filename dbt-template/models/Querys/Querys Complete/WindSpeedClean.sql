{{ config(materialized='ephemeral')}}
-- Se toman los datos necesarios despues de realizar la limpieza en la tabla velocidad del viento
SELECT Fecha, Codigo, Region, Departamento, Municipio, PromedioLa AS Latitud, PromedioLo AS Longitud, PromedioVel AS Velocidad
FROM
{{ ref('PromByHours') }}
ORDER BY
Fecha