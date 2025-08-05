{{
    config(
        materialized = 'table',
    )
}}
with raw_tags as (
    select * from movielens.raw.raw_tags
)
select 
    userid as user_id,
    movieid as movie_id,
    tag,
    to_TIMESTAMP_LTZ(timestamp) as tag_timestamp
from raw_tags