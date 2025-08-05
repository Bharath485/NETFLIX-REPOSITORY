{{
      config(
        materialized = 'incremental',
        on_schema_change = 'fail',
    )
}}

WITH SRC_RATINGS AS (
    SELECT * FROM {{ ref('src_ratings') }}
)
SELECT USER_ID, MOVIE_ID, RATING, rating_timestamp FROM src_ratings
WHERE RATING IS NOT NULL 

{% if is_incremental()%}
    and rating_timestamp > (select max(rating_timestamp) from {{ this }})
{% endif %}