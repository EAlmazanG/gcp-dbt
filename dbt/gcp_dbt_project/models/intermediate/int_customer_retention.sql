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
            ordered_at
        from {{ ref('base_raw_streaming__orders') }}
    ),

    orders_by_month as (
        select
            customer_id,
            format_date('%Y-%m', ordered_at) as year_month
        from orders
        group by customer_id, year_month
    ),

    months_per_customer as (
        select
            customer_id,
            min(date_trunc(cast(ordered_at as date), month)) as first_month,
            max(date_trunc(cast(ordered_at as date), month)) as last_month
        from orders
        group by customer_id
    ),

    all_months as (
        select
            customer_id,
            format_date('%Y-%m', month) as year_month
        from months_per_customer,
        unnest(generate_date_array(first_month, last_month, interval 1 month)) as month
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

select * from order_months_check
