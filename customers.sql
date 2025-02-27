with orders_per_customer as (
    select 
        customer_id,
        min(created_at) as first_order_at, 
        count(id) as number_of_orders 
    from `analytics-engineers-club.coffee_shop.orders`
    group by 1
)

select
    customer_id,     
    customers.name, 
    customers.email,
    orders_per_customer.first_order_at,
    orders_per_customer.number_of_orders
from `analytics-engineers-club.coffee_shop.customers` customers
inner join orders_per_customer on customers.id = orders_per_customer.customer_id
order by orders_per_customer.first_order_at 
limit 5
