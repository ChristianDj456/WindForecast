SELECT
  v.*,
  d.PromedioDir AS ValorObservadoDireccion
FROM
  {{ ref('MedianaVel') }} AS v,
  {{ ref('MedianaDir') }} AS d