{{ config(materialized='table') }}
WITH MediaCalc AS (
  SELECT AVG(PromedioDir) AS Media
  FROM {{ ref('PromHorasDir') }}
)

SELECT DISTINCT
  pc.*,
  mc.Media AS Media
FROM 
  {{ ref('PromHorasDir') }} pc
CROSS JOIN 
  MediaCalc AS mc