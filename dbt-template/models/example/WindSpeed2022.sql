{{ config(materialized='view') }}

SELECT *
FROM {{ source('Proyect', 'Vwinds22Complete') }}