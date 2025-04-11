{{ 
    config(
        materialized='table',
    ) 
}}

with
    order_items as (
        select
            item_id,
            order_id,
            product_id,
            store_id,
            customer_id,
            ordered_at,
            day_ordered_on,
            week_ordered_on,
            month_ordered_on,
            product_name,
            product_type,
            is_item_food,
            is_item_drink,
            product_price_eur,
            total_supply_cost_eur,
            is_perishable_product
        from
            {{ ref('int_order_items') }}
    ),

    stores as (
        select store_id, store_name from {{ ref('int_dim_stores') }}
    ),

    customers as(
        select
            customer_id,
            customer_category
        from {{ ref('int_customers') }}
    ),

    combination as (
        select
            order_items.item_id,
            order_items.order_id,
            order_items.product_id,
            order_items.store_id,
            stores.store_name,
            order_items.customer_id,
            customers.customer_category,
            order_items.ordered_at,
            order_items.day_ordered_on,
            order_items.week_ordered_on,
            order_items.month_ordered_on,
            order_items.product_name,
            order_items.product_type,
            order_items.is_item_food,
            order_items.is_item_drink,
            order_items.product_price_eur,
            order_items.total_supply_cost_eur,
            order_items.is_perishable_product
        from order_items
        left join stores on
            stores.store_id = order_items.store_id
        left join customers on
            customers.customer_id = order_items.customer_id
    )

select * from combination