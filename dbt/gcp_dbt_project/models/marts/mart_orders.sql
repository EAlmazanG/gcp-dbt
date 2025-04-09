{{ 
    config(
        materialized='view',
    ) 
}}

select * from {{ref('int_fact_orders')}}