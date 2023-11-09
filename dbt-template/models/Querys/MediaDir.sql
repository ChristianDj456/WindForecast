WITH MediaCalc AS (
  SELECT AVG(PromedioDir) AS Media
  FROM {{ ref('PromHorasDir') }}
)

SELECT 
  pc.*,
  mc.Media AS Media
FROM 
  {{ ref('PromHorasDir') }} pc
CROSS JOIN 
  MediaCalc AS mc