{{ config(materialized='ephemeral')}}

SELECT Fecha, Codigo, Region, Departamento, Municipio, PromedioLaDir AS Latitud, PromedioLoDir AS Longitud, PromedioDir AS Direccion
FROM
{{ ref('PromHorasDir') }}
ORDER BY
Fecha