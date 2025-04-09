{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_batch', 'products') }})

select
    cast(sku as string) as product_id,
    cast(type as string) as product_type,
    cast(name as string) as product_name,
    cast(description as string) as product_description,
    round(cast(price / 100 as float64), 2) as product_price_eur,
    ifnull(type = 'jaffle', false) as is_item_food,
    ifnull(type = 'beverage', false) as is_item_drink
from source