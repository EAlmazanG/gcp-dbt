{{ config(
    materialized='table',
    schema='dbt_staging'
) }}

select *
from `gcp-dbt-454911.raw_batch.customers`