with raw_links as (
    select * from movielens.raw.raw_links
)
select 
    movieid as movie_id,
    imdbid as imdb_id,
    tmdbid as tmdb_id
from raw_links