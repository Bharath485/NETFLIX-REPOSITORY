{{ config(
    materialized='view'
) }}

with current_time_data as (
    SELECT
        CURRENT_TIMESTAMP() AS current_time,
        'Bharath' AS Bharath
)

SELECT * FROM current_time_data
