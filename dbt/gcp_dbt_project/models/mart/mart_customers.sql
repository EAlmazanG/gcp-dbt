{{ 
    config(
        materialized='view',
    ) 
}}

select * from {{ref('int_customers')}}