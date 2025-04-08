{{ 
    config(
        materialized='table',
    ) 
}}

with
    products as (
        select * from {{ ref('base_raw_batch__products') }}
    )

select * from products