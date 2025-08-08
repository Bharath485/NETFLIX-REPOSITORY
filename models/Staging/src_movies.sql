WITH RAW_MOVIES AS (
    SELECT * FROM {{source('netflix', 'r_movies')}}
)
SELECT 
    MOVIEID AS MOVIE_ID,
    TITLE,
    GENRES
FROM RAW_MOVIES