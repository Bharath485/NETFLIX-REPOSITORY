WITH RATINGS AS (
    SELECT DISTINCT USER_ID FROM {{ ref('src_ratings') }}
),

tags as (
    select distinct user_id from {{ ref('src_tags')}}
)

select distinct user_id
from (
    select * from RATINGS
    union all
    select * from tags
)