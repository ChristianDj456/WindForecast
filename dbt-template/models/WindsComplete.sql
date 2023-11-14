{{ config(materialized='table') }}
-- Se unen las tablas de Velocidad y Dirección del viento
SELECT  DISTINCT speed_clean.* EXCEPT(Velocidad), Velocidad, Direccion 
FROM {{ ref('WindSpeedClean') }} speed_clean
INNER JOIN {{ ref('WindsDirectClean') }} direction_clean
USING(Fecha, Codigo)
ORDER BY Fecha, Codigo