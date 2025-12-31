{{ config(materialized='table') }}

with source_data as (
    select
        snapshot_date,
        store_code,
        product_code,
        on_hand_qty,
        on_order_qty
    from {{ source('staging', 'stg_inventory') }}
),

store_lookup as (
    select
        store_code,
        store_key
    from {{ ref('dim_store') }}
),

product_lookup as (
    select
        product_code,
        product_key
    from {{ ref('dim_product') }}
),

final as (
    select
        cast(to_char(current_date(), 'YYYYMMDD') as integer) as date_key,
        cast(s.snapshot_date as date) as dt_of_snapshot,
        coalesce(st.store_key, -1) as store_key,
        coalesce(p.product_key, -1) as product_key,
        cast(coalesce(s.on_hand_qty, 0) as integer) as on_hand_qty,
        cast(coalesce(s.on_order_qty, 0) as integer) as on_order_qty,
        cast(current_timestamp() as timestamp) as dt_created,
        cast(current_timestamp() as timestamp) as dt_modified
    from store_lookup st
    right join source_data s
        on st.store_code = s.store_code
    right join product_lookup p
        on p.product_code = s.product_code
)

select
    date_key,
    dt_of_snapshot,
    store_key,
    product_key,
    on_hand_qty,
    on_order_qty,
    dt_created,
    dt_modified
from final