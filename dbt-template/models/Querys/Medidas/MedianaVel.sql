

WITH MedianaCalc AS (
  SELECT APPROX_QUANTILES(PromedioVel, 2)[OFFSET(1)] AS Mediana
  FROM {{ ref('MediaVel') }}
)

SELECT 
  * EXCEPT(Mediana), -- Asegúrate de incluir todas las columnas necesarias de MediaVel
  m.Mediana
FROM 
  {{ ref('MediaVel') }} mv
CROSS JOIN 
  MedianaCalc AS m
