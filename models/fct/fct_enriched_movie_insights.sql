### Generated DBT Code ###

-- This model enriches movie insights by joining movie details with additional data sources

WITH movie_details AS (
    SELECT
        movie_id,
        title,
        release_year,
        genre
    FROM {{ ref('dim_movies') }}
),

additional_data AS (
    SELECT
        movie_id,
        rating,
        box_office
    FROM {{ ref('dim_additional_movie_data') }}
)

SELECT
    md.movie_id,
    md.title,
    md.release_year,
    md.genre,
    ad.rating,
    ad.box_office
FROM movie_details md
LEFT JOIN additional_data ad ON md.movie_id = ad.movie_id