{{ config(
    materialized='table',
) }}

select
    string_field_0 as customer_id,
    string_field_1 as customer_name
from `gcp-dbt-454911.raw_batch.customers`