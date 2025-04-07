{{ config(
    materialized='table',
) }}

with source as (select * from {{ source('raw_streaming', 'items') }})

select
  json_value(cast(message as string), '$.id') as item_id,
  json_value(cast(message as string), '$.order_id') as order_id,
  json_value(cast(message as string), '$.sku') as product_id,
  timestamp_millis(timestamp) AS pubsub_event_timestamp
from source