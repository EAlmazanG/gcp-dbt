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
            order_total_eur        
        from {{ ref('base_raw_streaming__orders') }}
    ),

    orders_summary as (
        select
            orders.customer_id,
            count(distinct orders.order_id) as number_lifetime_orders,
            count(distinct orders.order_id) > 1 as is_repeat_buyer,
            min(orders.ordered_at) as first_ordered_at,
            max(orders.ordered_at) as last_ordered_at,
            sum(orders.subtotal) as total_pretax_purchases_eur,
            sum(orders.tax_paid) as total_tax_purchases_eur,
            sum(orders.order_total) as total_purchases_eur
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
                when orders_summary.is_repeat_buyer then 'returning'
                else 'new'
            end as customer_type
        from customers
        left join orders_summary on
            customers.customer_id = orders_summary.customer_id
    )

select * from combination