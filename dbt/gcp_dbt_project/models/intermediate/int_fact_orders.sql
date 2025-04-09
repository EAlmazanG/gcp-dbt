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
            order_total_eur,
            total_order_cost_eur,
            total_order_revenue_eur,
            total_order_profit_eur,
            number_order_items,
            number_order_perishable_items,
            customer_order_number    
        from {{ ref('int_orders') }}
    ),

    customers as(
        select
            customer_id,
            customer_category
        from {{ ref('int_customers') }}
    ),

    stores as (
        select
            store_id,
            store_name
        from {{ ref('int_dim_stores') }}
    ),

    combination as (
        select
            order_id,
            stores.store_id,
            stores.store_name,
            customers.customer_id,
            customers.customer_category,
            ordered_at,
            order_subtotal_eur,
            order_tax_paid_eur,
            order_total_eur,
            total_order_cost_eur,
            total_order_revenue_eur,
            total_order_profit_eur,
            number_order_items,
            number_order_perishable_items,
            customer_order_number    
        from orders
            left join customers on
                orders.customer_id = customers.customer_id
            left join stores on
                orders.store_id = stores.store_id
    )

select * from combination