{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_batch', 'customers') }})

select
    cast(string_field_0 as string) as customer_id,
    cast(string_field_1 as string) as customer_name
from source
where string_field_0 <> 'string_field_0'