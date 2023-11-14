{{ config(materialized='view') }}
SELECT DISTINCT DATE(Fecha) AS Fecha,AVG(Velocidad) OVER (PARTITION BY DATE(Fecha)) AS MediaViento -- Se toman los datos de la
-- fecha y la media
FROM {{ ref('WindsComplete') }}
WHERE EXTRACT(YEAR FROM TIMESTAMP(Fecha)) = 2021 -- Se filtran los resultados para incluir solo aquellos registros 
-- cuyo año extraído de la columna "Fecha" sea 2021.
ORDER BY Fecha ASC