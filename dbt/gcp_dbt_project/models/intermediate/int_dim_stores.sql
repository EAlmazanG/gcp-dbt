{{ 
    config(
        materialized='table',
    ) 
}}

with
    stores as (
        select * from {{ ref('base_raw_batch__stores') }}
    )

select * from stores