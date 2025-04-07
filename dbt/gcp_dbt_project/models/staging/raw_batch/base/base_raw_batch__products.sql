{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_batch', 'products') }})

select
    cast(sku as string) as product_id,
    cast(name as string) as product_name,
    cast(description as string) as product_description,
    cast(price / 100 as int64) as product_price_eur
from source