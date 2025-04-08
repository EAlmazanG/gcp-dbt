{{
  config(
        materialized = 'table',
    )
}}

with 
    date_spine as ( 
        {{  
            dbt_utils.date_spine(
                start_date = "to_date('01/01/1989', 'mm/dd/yyyy')",
                datepart = "day",
                end_date = "dateadd(year, 10, current_date)"
            )
        }} 
    ),
     
    calculated as (
        select
            date_day,
            format_date('%A', date_day) as day_name,
            format_date('%a', date_day) as short_day_name,
            case
                when format_date('%A', date_day) = 'Monday' then 1
                when format_date('%A', date_day) = 'Tuesday' then 2
                when format_date('%A', date_day) = 'Wednesday' then 3
                when format_date('%A', date_day) = 'Thursday' then 4
                when format_date('%A', date_day) = 'Friday' then 5
                when format_date('%A', date_day) = 'Saturday' then 6
                when format_date('%A', date_day) = 'Sunday' then 7
            end as day_of_week,
            date_trunc(date_day, week) as first_day_of_week,
            last_value(date_day) over (
                partition by date_trunc(date_day, week)
                order by date_day
                rows between unbounded preceding and unbounded following
            ) as last_day_of_week,
            extract(week from date_day) as week_of_year,
            extract(month from date_day) as month_number,
            extract(day from date_day) as day_of_month,
            format_date('%B', date_day) as month_name,
            format_date('%b', date_day) as short_month_name,
            date_trunc(date_day, month) as first_day_of_month,
            date_sub(date_add(date_trunc(date_day, month), interval 1 month), interval 1 day) as last_day_of_month,
            extract(year from date_day) as year_number,
            row_number() over (
                partition by extract(year from date_day)
                order by date_day
            ) as day_of_year,
            date_trunc(date_day, year) as first_day_of_year,
            last_value(date_day) over (
                partition by extract(year from date_day)
                order by date_day
                rows between unbounded preceding and unbounded following
            ) as last_day_of_year,

            extract(quarter from date_day) as quarter_number,
            row_number() over (
                partition by extract(year from date_day), extract(quarter from date_day)
                order by date_day
            ) as day_of_quarter,
            date_trunc(date_day, quarter) as first_day_of_quarter,
            last_value(date_day) over (
                partition by extract(year from date_day), extract(quarter from date_day)
                order by date_day
                rows between unbounded preceding and unbounded following
            ) as last_day_of_quarter,
            concat(cast(extract(year from date_day) as string), '-Q', cast(extract(quarter from date_day) as string)) as quarter_name,
            case
                when extract(dayofweek from date_day) in (1, 7) then 1
                else 0
            end as is_weekend
        from date_spine
    )

select 
    date_day,
    day_name,
    short_day_name,
    day_of_week,

    first_day_of_week,
    last_day_of_week,
    week_of_year,

    month_name,
    short_month_name,
    month_number,
    day_of_month,
    first_day_of_month,
    last_day_of_month,

    quarter_name,
    quarter_number,
    day_of_quarter,
    first_day_of_quarter,
    last_day_of_quarter,

    year_number,
    day_of_year,
    first_day_of_year,
    last_day_of_year,
    is_weekend

from calculated