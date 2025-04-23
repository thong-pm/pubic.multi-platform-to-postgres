{{ config(
    schema='wrike',
    materialized='view'
) }}

SELECT
    id,
    createddate,
    completeddate,
    updateddate,
    title,
    status,
    importance,
    ROUND(EXTRACT(EPOCH FROM (completeddate - createddate)) / 86400, 4) AS duration_in_days
FROM {{ source('wrike', 'tasks') }}
WHERE status = 'Completed'
  AND createddate IS NOT NULL
  AND completeddate IS NOT NULL
  AND LOWER(title) LIKE '%quote%'
