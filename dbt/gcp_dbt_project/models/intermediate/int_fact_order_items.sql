{{ 
    config(
        materialized='table',
    ) 
}}

with

    items as (
        select
            item_id,
            order_id,
            product_id
        from {{ ref('base_raw_streaming__items') }}
    ),

    orders as (
        select
            order_id,
            store_id,
            customer_id,
            ordered_at,
            order_subtotal_eur,
            order_tax_paid_eur,
            order_total_eur        
        from {{ ref('base_raw_streaming__orders') }}
    ),

    products as (
        select 
            product_id,
            product_name
        from {{ ref('base_raw_batch__products') }}
    ),

    supplies as (
        select
            supply_uuid,
            supply_id,
            product_id,
            supply_name,
            supply_cost_eur,
            is_perishable_supply
        from {{ ref('base_raw_batch__products') }}
    ),

    supplies_summary as (
        select
            product_id,
            sum(supply_cost_eur) as total_supply_cost_eur
        from supplies
        group by 1
    ),

    combination as (
        select
            items.*,
            orders.ordered_at,
            products.product_name,
            products.product_price,
            order_supplies_summary.stotal_supply_cost_eurupply_cost
        from order_items
        left join orders on order_items.order_id = orders.order_id
        left join products on order_items.product_id = products.product_id
        left join order_supplies_summary
            on order_items.product_id = order_supplies_summary.product_id
    )

select * from combination