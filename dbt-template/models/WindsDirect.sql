{{ config(materialized='ephemeral') }}
SELECT *
FROM {{ source('WindsForecast', 'WindsDirect_EXT') }}