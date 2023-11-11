{{ config(materialized='view') }}
SELECT DISTINCT DATE(Fecha) AS Fecha,AVG(Velocidad) OVER (PARTITION BY DATE(Fecha)) AS MediaViento
FROM {{ ref('WindsComplete') }}
WHERE EXTRACT(YEAR FROM TIMESTAMP(Fecha)) = 2022
ORDER BY Fecha ASC