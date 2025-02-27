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
    coalesce(orders_per_customer.number_of_orders, 0) as n_orders,
    orders_per_customer.number_of_orders
from `analytics-engineers-club.coffee_shop.customers` customers
left join orders_per_customer on customers.id = orders_per_customer.customer_id
order by orders_per_customer.first_order_at 

--comment to test

