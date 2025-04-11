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
            customer_id,
            cast(ordered_at as date) as ordered_date
        from {{ ref('base_raw_streaming__orders') }}
    ),

    orders_by_month as (
        select
            customer_id,
            format_date('%Y-%m', ordered_date) as year_month
        from orders
        group by customer_id, year_month
    ),

    months_per_customer as (
        select
            customer_id,
            min(date_trunc(ordered_date, month)) as month_first_customer_transaction,
            max(date_trunc(ordered_date, month)) as month_last_customer_transaction,
            {{ dataset_current_date_query }} as month_last_dataset,
            min(ordered_date) as day_first_customer_transaction,
            max(ordered_date) as day_last_customer_transaction,
            min(date_trunc(ordered_date, week)) as week_first_customer_transaction, 
            max(date_trunc(ordered_date, week)) as week_last_customer_transaction
        from orders
        group by customer_id
    ),

    all_months as (
        select
            customer_id,
            format_date('%Y-%m', month) as year_month
        from months_per_customer,
        unnest(generate_date_array(month_first_customer_transaction, month_last_dataset, interval 1 month)) as month
    ),

    order_months_check as (
        select
            all_months.customer_id,
            count(distinct all_months.year_month) = count(distinct orders_by_month.year_month) as has_order_every_month
        from all_months
        left join orders_by_month
            on all_months.customer_id = orders_by_month.customer_id
            and all_months.year_month = orders_by_month.year_month
        group by all_months.customer_id
    )

select
    order_months_check.customer_id,
    order_months_check.has_order_every_month,
    months_per_customer.day_first_customer_transaction,
    months_per_customer.day_last_customer_transaction,
    months_per_customer.week_first_customer_transaction,
    months_per_customer.week_last_customer_transaction,
    months_per_customer.month_first_customer_transaction,
    months_per_customer.month_last_customer_transaction
from order_months_check
left join months_per_customer
    on order_months_check.customer_id = months_per_customer.customer_id
