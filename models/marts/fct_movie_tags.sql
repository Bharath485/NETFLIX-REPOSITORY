{{ config(materialized='table') }}

with source_data as (
    select
        movieid,
        tagid,
        userid,
        timestamp
    from {{ ref('stg_movie_tags') }}
),

date_lookup as (
    select
        date_key,
        date_value
    from {{ ref('dim_date') }}
),

movie_lookup as (
    select
        movie_key,
        movie_bk
    from {{ ref('dim_movie') }}
),

tag_lookup as (
    select
        tag_key,
        tag_bk
    from {{ ref('dim_tag') }}
),

user_lookup as (
    select
        user_key,
        user_bk
    from {{ ref('dim_user') }}
),

final as (
    select
        cast(d.date_key as integer) as tag_date_key,
        cast(m.movie_key as integer) as movie_key,
        cast(t.tag_key as integer) as tag_key,
        cast(u.user_key as integer) as user_key,
        cast(current_timestamp() as timestamp) as dt_created,
        cast(current_timestamp() as timestamp) as dt_modified
    from source_data s
    left join date_lookup d 
        on cast(to_char(to_timestamp(s.timestamp), 'YYYYMMDD') as integer) = d.date_value
    left join movie_lookup m 
        on cast(s.movieid as varchar) = cast(m.movie_bk as varchar)
    left join tag_lookup t 
        on cast(s.tagid as varchar) = cast(t.tag_bk as varchar)
    left join user_lookup u 
        on cast(s.userid as varchar) = cast(u.user_bk as varchar)
)

select
    tag_date_key,
    movie_key,
    tag_key,
    user_key,
    dt_created,
    dt_modified
from final