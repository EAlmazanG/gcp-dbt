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
            product_price_eur,
            total_supply_cost_eur
        from {{ ref('int_fact_order_items') }}
    ),

    order_items_summary as (
        select
            order_id,
            sum(total_supply_cost_eur) as total_order_cost_eur,
            sum(product_price_eur) as total_order_revenue_eur,
            count(item_id) as number_order_items
        from order_items
        group by 1
    ),

    combination as (
        select
            orders.order_id,
            orders.store_id,
            orders.customer_id,
            orders.ordered_at,
            orders.order_subtotal_eur,
            orders.order_tax_paid_eur,
            orders.order_total_eur,
            order_items_summary.total_order_cost_eur,
            order_items_summary.total_order_revenue_eur,
            order_items_summary.total_order_revenue_eur - order_items_summary.total_order_cost_eur as total_order_profit_eur,
            order_items_summary.number_order_items,
            row_number() over (
                partition by orders.customer_id
                order by orders.ordered_at asc
            ) as customer_order_number
        from orders
        left join order_items_summary on
            orders.order_id = order_items_summary.order_id
    )

select * from combination