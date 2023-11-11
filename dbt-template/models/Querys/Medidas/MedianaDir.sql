{{ config(materialized='view')}}
WITH MedianaCalc AS (
  SELECT APPROX_QUANTILES(PromedioDir, 2)[OFFSET(1)] AS Mediana
  FROM {{ ref('MediaDir') }}
)

SELECT 
  * EXCEPT(Mediana),
  m.Mediana
FROM 
  {{ ref('MediaDir') }} mv
CROSS JOIN 
  MedianaCalc AS m
