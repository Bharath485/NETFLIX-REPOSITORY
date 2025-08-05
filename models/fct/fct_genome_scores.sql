with src_scores as (
    select * from {{ ref('src_genome_scores') }}
)

select movie_id, tag_id, round(RELEVANCE,4) as relevance_score
from src_scores
where RELEVANCE > 0