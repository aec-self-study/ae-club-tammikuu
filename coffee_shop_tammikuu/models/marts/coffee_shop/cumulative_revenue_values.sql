{{ config(
    materialized='table'
 ) }}

with customer_weeks as (
    select 
        weekly_revenues.customer_id,
        all_weeks.date_week
    from 
        {{ ref('weekly_revenues') }} as weekly_revenues    
    left join 
        {{ ref('all_weeks') }} as all_weeks 
    on 
        all_weeks.date_week >= weekly_revenues.orders_date_week
    where weekly_revenues.customer_type_for_order = 'New'
    group by 1, 2
), 

totals_per_week as (
    select
        customer_id, 
        orders_date_week,
        sum(price_for_week) as weekly_total
    from
        {{ ref('weekly_revenues') }} as weekly_revenues
    group by 1, 2
)

select 
    customer_weeks.customer_id, 
    date_week, 
    coalesce(weekly_total, 0) as weekly_revenue,
    sum(coalesce(weekly_total, 0)) over (partition by totals_per_week.customer_id order by date_week rows between unbounded preceding and current row) as cumulative_revenue
from 
    customer_weeks 
left join 
    totals_per_week
on customer_weeks.customer_id = totals_per_week.customer_id 
and customer_weeks.date_week = totals_per_week.orders_date_week
