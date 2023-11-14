{{ config(materialized='ephemeral')}}
-- Se toman los datos necesarios despues de realizar la limpieza en la tabla dirección del viento
SELECT Fecha, Codigo, Region, Departamento, Municipio, PromedioLaDir AS Latitud, PromedioLoDir AS Longitud, PromedioDir AS Direccion
FROM
{{ ref('PromHorasDir') }}
ORDER BY
Fecha