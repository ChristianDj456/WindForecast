WITH MediaCalc AS (
SELECT AVG(Viento) AS Median
FROM {{ ref('PromByHours') }}
)

SELECT f.*, me.Median AS Media
FROM {{ ref('PromByHours') }}
CROSS JOIN MediaCalc AS me