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
        cast(to_char(current_date(), 'YYYYMMDD') as varchar) as date_key,
        cast(s.snapshot_date as date) as dt_of_snapshot,
        coalesce(st.store_key, -1) as store_key,
        coalesce(p.product_key, -1) as product_key,
        cast(coalesce(s.on_hand_qty, 0) as integer) as on_hand_qty,
        cast(coalesce(s.on_order_qty, 0) as integer) as on_order_qty,
        cast(current_timestamp() as timestamp) as dt_created,
        cast(current_timestamp() as timestamp) as dt_modified
    from source_data s
    left join store_lookup st on s.store_code = st.store_code
    left join product_lookup p on s.product_code = p.product_code
)

select * from final