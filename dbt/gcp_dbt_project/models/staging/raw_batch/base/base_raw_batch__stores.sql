{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_batch', 'stores') }})

select
    cast(id as string) as store_id,
    cast(name as string) as store_name,
    cast(opened_at as timestamp) as store_opened_at,
    cast(tax_rate as float64) as store_tax_rate_percentage
from source