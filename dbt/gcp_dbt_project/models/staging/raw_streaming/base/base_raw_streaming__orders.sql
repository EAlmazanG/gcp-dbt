{{ config(
    materialized='table',
) }}

with source as (select *, cast(message as string) as message_json from {{ source('raw_streaming', 'orders') }})

select
  timestamp_millis(timestamp) as pubsub_event_timestamp,
  json_value(message_json, '$.id') as order_id,
  json_value(message_json, '$.store_id') as store_id,
  json_value(message_json, '$.customer') as customer_id,
  parse_timestamp('%FT%T', json_value(message_json, '$.ordered_at')) as ordered_at,
  cast(cast(json_value(message_json, '$.subtotal') as float64) / 100 as float64) as order_subtotal_eur,
  cast(cast(json_value(message_json, '$.tax_paid') as float64) / 100 as float64) as order_tax_paid_eur,
  cast(cast(json_value(message_json, '$.order_total') as float64) / 100 as float64) as order_total_eur
from source