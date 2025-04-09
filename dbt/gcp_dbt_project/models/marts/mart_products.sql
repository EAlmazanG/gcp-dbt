{{ 
    config(
        materialized='view',
    ) 
}}

select * from {{ref('int_fact_order_items')}}