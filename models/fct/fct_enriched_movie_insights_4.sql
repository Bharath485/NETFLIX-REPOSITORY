### Generated DBT Code ###

-- This model calculates enriched movie insights

WITH base_data AS (
    SELECT
        movie_id,
        title,
        release_year,
        genre,
        rating
    FROM {{ ref('stg_movies') }}
),

aggregated_data AS (
    SELECT
        genre,
        AVG(rating) AS avg_rating,
        COUNT(movie_id) AS total_movies
    FROM base_data
    GROUP BY genre
)

SELECT
    bd.movie_id,
    bd.title,
    bd.release_year,
    bd.genre,
    bd.rating,
    ad.avg_rating,
    ad.total_movies
FROM base_data bd
LEFT JOIN aggregated_data ad
ON bd.genre = ad.genre
