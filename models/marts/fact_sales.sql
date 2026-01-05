{{ config(materialized='table') }}

with source_data as (
    select
        sale_date,
        store_code,
        product_code,
        coalesce(sale_qty, 0) as sale_qty
    from {{ ref('stg_sales') }}
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
        cast(to_char(s.sale_date, 'YYYYMMDD') as integer) as sale_date_key,
        coalesce(st.store_key, -1) as store_key,
        coalesce(p.product_key, -1) as product_key,
        cast(s.sale_qty as integer) as sale_qty,
        current_timestamp() as dt_created,
        current_timestamp() as dt_modified
    from source_data s
    left join store_lookup st
        on s.store_code = st.store_code
    left join product_lookup p
        on s.product_code = p.product_code
)

select * from final