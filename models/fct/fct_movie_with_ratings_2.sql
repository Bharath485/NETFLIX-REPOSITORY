{{ config(materialized="table") }}

with stg_inventory as (
    select * from {{ ref('STG_INVENTORY') }}
),

dim_store as (
    select * from {{ ref('DIM_STORE') }}
),

dim_product as (
    select * from {{ ref('DIM_PRODUCT') }}
)

select
    -- Date key in YYYYMMDD format
    cast(to_char(current_date, 'YYYYMMDD') as integer) as DATE_KEY,
    
    -- Snapshot date
    cast(SNAPSHOT_DATE as date) as DT_OF_SNAPSHOT,
    
    -- Store key lookup
    coalesce(ds.store_key, -1) as STORE_KEY,
    
    -- Product key lookup  
    coalesce(dp.product_key, -1) as PRODUCT_KEY,
    
    -- On hand quantity with default 0
    coalesce(cast(ON_HAND_QTY as integer), 0) as ON_HAND_QTY,
    
    -- On order quantity with default 0
    coalesce(cast(ON_ORDER_QTY as integer), 0) as ON_ORDER_QTY,
    
    -- Audit timestamps
    current_timestamp as DT_CREATED,
    current_timestamp as DT_MODIFIED

from stg_inventory si
left join dim_store ds 
    on si.STORE_CODE = ds.store_code
left join dim_product dp 
    on si.PRODUCT_CODE = dp.product_code