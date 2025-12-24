{{ config(materialized="table") }}

with source_data as (
    select
        -- Date key in YYYYMMDD format
        cast(to_char(snapshot_date, 'YYYYMMDD') as integer) as date_key,
        
        -- Snapshot date
        cast(snapshot_date as date) as dt_of_snapshot,
        
        -- Store key lookup
        ds.store_key,
        
        -- Product key lookup  
        dp.product_key,
        
        -- Inventory quantities with default 0
        coalesce(cast(on_hand_qty as integer), 0) as on_hand_qty,
        coalesce(cast(on_order_qty as integer), 0) as on_order_qty,
        
        -- Audit timestamps
        current_timestamp() as dt_created,
        current_timestamp() as dt_modified
        
    from {{ ref('stg_inventory') }} si
    left join {{ ref('dim_store') }} ds 
        on si.store_code = ds.store_code
    left join {{ ref('dim_product') }} dp 
        on si.product_code = dp.product_code
)

select * from source_data