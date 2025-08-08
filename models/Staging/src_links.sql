with raw_links as (
    select * from {{source('netflix', 'r_links')}}
)
select 
    movieid as movie_id,
    imdbid as imdb_id,
    tmdbid as tmdb_id
from raw_links