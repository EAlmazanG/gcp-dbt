{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_batch', 'supplies') }})

select
    cast(id as string) as supply_id,
    cast(sku as string) as product_id,
    {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,
    cast(name as string) as supply_name,
    cast(cost/100) as supply_cost_eur,
    perishable as is_perishable
from source