{{ config(
    materialized='view'
 ) }}

with weekly_prices as (
    select
        date_trunc(orders.created_at, year) as orders_date_year,
        date_trunc(orders.created_at, week) as orders_date_week,
        orders.order_id,
        orders.customer_id,
        order_items.product_id, 
        product_prices.price as price_for_week,
        products.category
    from {{ ref('stg_coffee_shop__orders') }} as orders
    left join {{ ref('stg_coffee_shop__order_items') }} as order_items
        on orders.order_id = order_items.order_id
    left join {{ ref('stg_coffee_shop__products') }} as products
        on order_items.product_id = products.product_id
    left join {{ ref('stg_coffee_shop__product_prices') }} as product_prices
        on products.product_id = product_prices.product_id
    where 
        orders.created_at between product_prices.created_at and product_prices.ended_at
),

first_order as (
    select
        customers.customer_id, 
        orders.order_id,
        orders.created_at as date_of_order,
        min(orders.created_at) over (partition by customers.customer_id) as date_of_first_order
    from
        {{ ref('stg_coffee_shop__customers') }} as customers
    left join orders on customers.customer_id = orders.customer_id
)


select 
    customers.customer_id,
    case when date_of_first_order = date_of_this_order then 'New' 
        else 'Returning'
    end as customer_type_for_order,
    weekly_prices.order_id,
    weekly_prices.product_id,
    weekly_prices.category,
    weekly_prices.orders_date_year,
    weekly_prices.orders_date_week,
    weekly_prices.price_for_week,
    first_order.date_of_order,
    first_order.date_of_first_order
from {{ ref('stg_coffee_shop__customers') }} as customers 
left join weekly_prices 
    on customers.customer_id = weekly_prices.customer_id
left join first_order 
    on customers.customer_id = first_order.customer_id


