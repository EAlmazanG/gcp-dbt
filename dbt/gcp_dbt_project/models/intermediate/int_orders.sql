{{ 
    config(
        materialized='table',
    ) 
}}

with
    orders as (
        select
            order_id,
            store_id,
            customer_id,
            ordered_at,
            day_ordered_on,
            week_ordered_on,
            month_ordered_on,
            order_subtotal_eur,
            order_tax_paid_eur,
            order_total_eur        
        from {{ ref('base_raw_streaming__orders') }}
    ),

    order_items as (
        select
            item_id,
            order_id,
            product_id,
            ordered_at,
            product_name,
            product_type,
            is_item_food,
            is_item_drink,
            product_price_eur,
            total_supply_cost_eur,
            is_perishable_product
        from {{ ref('int_fact_order_items') }}
    ),

    order_items_summary as (
        select
            order_id,
            sum(total_supply_cost_eur) as total_order_cost_eur,
            sum(product_price_eur) as total_order_revenue_eur,
            count(item_id) as number_order_items,
            count(case when is_item_food then item_id else null end) as number_order_food_items,
            count(case when is_item_drink then item_id else null end) as number_order_drink_items,
            count(case when is_perishable_product then item_id else null end) as number_order_perishable_items
        from order_items
        group by 1
    ),

    stores as (
        select store_id, store_name from {{ ref('int_dim_stores') }}
    ),

    combination as (
        select
            orders.order_id,
            orders.store_id,
            stores.store_name,
            orders.customer_id,
            orders.ordered_at,
            orders.day_ordered_on,
            orders.week_ordered_on,
            orders.month_ordered_on,
            orders.order_subtotal_eur,
            orders.order_tax_paid_eur,
            orders.order_total_eur,
            ifnull(order_items_summary.total_order_cost_eur, 0) as total_order_cost_eur,
            ifnull(order_items_summary.total_order_revenue_eur, 0) as total_order_revenue_eur,
            ifnull(order_items_summary.total_order_revenue_eur - order_items_summary.total_order_cost_eur, 0) as total_order_profit_eur,
            ifnull(order_items_summary.number_order_items, 0) as number_order_items,
            ifnull(order_items_summary.number_order_drink_items, 0) as number_order_drink_items,
            ifnull(order_items_summary.number_order_food_items, 0) as number_order_food_items,
            ifnull(order_items_summary.number_order_perishable_items, 0) as number_order_perishable_items,
            row_number() over (
                partition by orders.customer_id
                order by orders.ordered_at asc
            ) as customer_order_number
        from orders
        left join order_items_summary on
            orders.order_id = order_items_summary.order_id
        left join stores on
            stores.store_id = orders.store_id
    )

select * from combination