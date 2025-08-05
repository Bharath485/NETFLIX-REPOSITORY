{{
    config(
        materialized = 'table'
    )
}}

with fact_ratings as (
    select* from {{ ref('fct_ratings') }}
),
seed_dates as (
    select * from {{ ref('seed_movie_release_dates')}}
)

select 
    f.*,
    case 
        when s.release_date is not null then s.release_date
        else '1900-01-01'::date
    end as release_info_avilable
from fct_ratings f
left join seed_dates s on f.movie_id = s.movie_id