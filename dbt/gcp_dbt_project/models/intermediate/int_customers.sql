{% set dataset_current_date_query %}
    (select max(cast(ordered_at as date)) from {{ ref('base_raw_streaming__orders') }})
{% endset %}

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
            day_ordered_on,
            week_ordered_on,
            month_ordered_on,
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

    orders_summary as (
        select
            orders.customer_id,
            count(distinct orders.order_id) as number_orders,
            sum(orders.number_order_items) as number_order_items,
            count(distinct orders.order_id) < 2 as is_new_customer,
            min(orders.ordered_at) as first_ordered_at,
            max(orders.ordered_at) as last_ordered_at,
            sum(orders.order_subtotal_eur) as total_pretax_purchases_eur,
            sum(orders.order_tax_paid_eur) as total_tax_purchases_eur,
            sum(orders.order_total_eur) as total_purchases_eur
        from orders
        group by 1
    ),

    customer_retention as (
        select
            customer_id,
            has_order_every_month,
            day_first_customer_transaction,
            day_last_customer_transaction,
            week_first_customer_transaction,
            week_last_customer_transaction,
            month_first_customer_transaction,
            month_last_customer_transaction
        from {{ ref('int_customer_retention') }}
    ),

    combination as (
        select
            customers.customer_id,
            orders_summary.total_pretax_purchases_eur,
            orders_summary.total_tax_purchases_eur,
            orders_summary.total_purchases_eur,
            orders_summary.number_orders,
            orders_summary.number_order_items,
            round(
                number_order_items / nullif(number_orders, 0),
                2
            ) as avg_items_per_order,
            orders_summary.first_ordered_at,
            orders_summary.last_ordered_at,
            orders_summary.is_new_customer,
            date_diff(cast(orders_summary.last_ordered_at as date), cast(orders_summary.first_ordered_at as date), day) as days_between_first_and_last_order,
            date_diff(
                {{ dataset_current_date_query }},
                cast(orders_summary.last_ordered_at as date),
                day
            ) > 30 as is_1m_churned,
            date_diff(
                {{ dataset_current_date_query }},
                cast(orders_summary.last_ordered_at as date),
                day
            ) > 90 as is_3m_churned,
            customer_retention.day_first_customer_transaction,
            customer_retention.day_last_customer_transaction,
            customer_retention.week_first_customer_transaction,
            customer_retention.week_last_customer_transaction,
            customer_retention.month_first_customer_transaction,
            customer_retention.month_last_customer_transaction,
            customer_retention.has_order_every_month,
            case
                when orders_summary.is_new_customer then 'new'
                when date_diff({{ dataset_current_date_query }}, cast(orders_summary.last_ordered_at as date), day) > 30 then '1m_churned'
                when date_diff({{ dataset_current_date_query }}, cast(orders_summary.last_ordered_at as date), day) > 90 then '3m_churned'
                when customer_retention.has_order_every_month then 'loyal'
                else 'recurrent'
            end as customer_category
        from customers
        left join orders_summary on
            customers.customer_id = orders_summary.customer_id
        left join customer_retention on
            customers.customer_id = customer_retention.customer_id
    )

select * from combination
