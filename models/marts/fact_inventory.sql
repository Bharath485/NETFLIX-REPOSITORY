{{ config(materialized='table') }}

with source_data as (
    select
        snapshot_date,
        store_code,
        product_code,
        on_hand_qty,
        on_order_qty
    from {{ ref('stg_inventory') }}
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
        to_char(current_date(), 'YYYYMMDD') as date_key,
        cast(s.snapshot_date as date) as dt_of_snapshot,
        coalesce(st.store_key, -1) as store_key,
        coalesce(p.product_key, -1) as product_key,
        coalesce(cast(s.on_hand_qty as integer), 0) as on_hand_qty,
        coalesce(cast(s.on_order_qty as integer), 0) as on_order_qty,
        current_timestamp() as dt_created,
        current_timestamp() as dt_modified
    from source_data s
    left join store_lookup st on s.store_code = st.store_code
    left join product_lookup p on s.product_code = p.product_code
)

select * from final