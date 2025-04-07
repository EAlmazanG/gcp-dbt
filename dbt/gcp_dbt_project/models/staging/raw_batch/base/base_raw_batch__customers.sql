{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_batch', 'customers') }})

select
    string_field_0 as customer_id,
    string_field_1 as customer_name
from source