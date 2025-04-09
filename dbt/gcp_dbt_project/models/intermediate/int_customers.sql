{{ 
    config(
        materialized='table',
    ) 
}}

with

    customers as (
        select
            customer_id,
            customer_name
        from {{ ref('base_raw_batch__customers') }}
    ),

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
        from {{ ref('int_fact_orders') }}
    ),

    orders_summary as (
        select
            orders.customer_id,
            count(distinct orders.order_id) as number_orders,
            count(distinct orders.order_id) < 2 as is_new_customer,
            min(orders.ordered_at) as first_ordered_at,
            max(orders.ordered_at) as last_ordered_at,
            sum(orders.order_subtotal_eur) as total_pretax_purchases_eur,
            sum(orders.order_tax_paid_eur) as total_tax_purchases_eur,
            sum(orders.order_total_eur) as total_purchases_eur
        from orders
        group by 1
    ),

    combination as (
        select
            customers.customer_id,
            orders_summary.number_lifetime_orders,
            orders_summary.first_ordered_at,
            orders_summary.last_ordered_at,
            orders_summary.total_pretax_purchases_eur,
            orders_summary.total_tax_purchases_eur,
            orders_summary.total_purchases_eur,
            case
                when orders_summary.is_churn_customer then 'churn'
                else 'new'
            end as customer_type
        from customers
        left join orders_summary on
            customers.customer_id = orders_summary.customer_id
    )

select * from combination