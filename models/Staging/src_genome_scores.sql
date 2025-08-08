with raw_genome_scores AS (
    select * from {{source('netflix', 'r_genome_scores')}}
)
select 
    movieid as movie_id,
    tagid as tag_id,
    relevance
from raw_genome_scores