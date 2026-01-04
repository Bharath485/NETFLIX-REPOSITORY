{{ config(materialized="table") }}

select
    cast(movieId as integer) as movie_id,
    cast(tagId as integer) as tag_id,
    cast(from_unixtime(timestamp) as date) as tag_date
from {{ source('raw', 'r_movie_tags') }}
;