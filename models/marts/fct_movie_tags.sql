{{ config(materialized="table") }}

select
    cast(movieid as integer) as movie_id,
    cast(tagid as integer) as tag_id,
    cast(to_timestamp(timestamp) as date) as tag_date
from {{ source('netflix', 'r_movie_tags') }}