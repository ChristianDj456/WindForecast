{{ config(materialized='ephemeral')}}
WITH MediaCalc AS (
  SELECT AVG(PromedioVel) AS Media
  FROM {{ ref('PromByHours') }}
)

SELECT  DISTINCT
  pc.*,
  mc.Media AS Media
FROM 
  {{ ref('PromByHours') }} pc
CROSS JOIN 
  MediaCalc AS mc