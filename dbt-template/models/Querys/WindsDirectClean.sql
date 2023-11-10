{{ config(materialized='table') }}

SELECT Fecha, Codigo, Region, Departamento, Municipio, PromedioLaDir AS Latitud, PromedioLoDir AS Longitud, PromedioDir AS Direccion
FROM
{{ ref('MedianaDir') }}
ORDER BY
Fecha