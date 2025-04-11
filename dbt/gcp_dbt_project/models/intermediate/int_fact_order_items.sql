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
            day_ordered_on,
            week_ordered_on,
            month_ordered_on,
            order_subtotal_eur,
            order_tax_paid_eur,
            order_total_eur        
        from {{ ref('base_raw_streaming__orders') }}
    ),

    products as (
        select 
            product_id,
            product_name,
            product_type,
            product_price_eur,
            is_item_food,
            is_item_drink
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
        from {{ ref('base_raw_batch__supplies') }}
    ),

    supplies_summary as (
        select
            product_id,
            sum(supply_cost_eur) as total_supply_cost_eur,
            count(distinct case when is_perishable_supply then supply_id else null end) as number_perishable_supplies,
            count(distinct case when not is_perishable_supply then supply_id else null end) as number_not_perishable_supplies,
        from supplies
        group by 1
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
            items.item_id,
            items.order_id,
            items.product_id,
            orders.store_id,
            stores.store_name,
            orders.customer_id,
            customers.customer_category,
            orders.ordered_at,
            orders.day_ordered_on,
            orders.week_ordered_on,
            orders.month_ordered_on,
            products.product_name,
            products.product_type,
            products.is_item_food,
            products.is_item_drink,
            products.product_price_eur,
            supplies_summary.total_supply_cost_eur,
            case when supplies_summary.number_perishable_supplies = 0 then false else true end as is_perishable_product
        from items
        left join orders on items.order_id = orders.order_id
        left join products on items.product_id = products.product_id
        left join supplies_summary
            on items.product_id = supplies_summary.product_id
        left join stores on
            stores.store_id = orders.store_id
        left join customers on
            customers.customer_id = orders.customers
    )

select * from combination