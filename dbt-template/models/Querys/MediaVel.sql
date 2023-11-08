WITH MediaCalc AS (
  SELECT AVG(PromedioVel) AS Media
  FROM {{ ref('PromByHours') }}
)

SELECT 
  pc.*,
  mc.Media AS Media
FROM 
  {{ ref('PromByHours') }} pc
CROSS JOIN 
  MediaCalc AS mc