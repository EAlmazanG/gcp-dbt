{{ config(
    materialized='table',
) }}

select
  timestamp_millis(timestamp) AS pubsub_event_timestamp,
  json_value(cast(message as string), '$.id') AS id,
  json_value(cast(message as string), '$.order_id') AS order_id,
  json_value(cast(message as string), '$.sku') AS sku
from `gcp-dbt-454911.raw_streaming.items`