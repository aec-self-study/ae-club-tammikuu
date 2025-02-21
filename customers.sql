select customer_id, name, email, min(created_at) as first_order_at, count(o.id) as number_of_orders from `analytics-engineers-club.coffee_shop.orders` o 
inner join `analytics-engineers-club.coffee_shop.customers` c on o.customer_id = c.id
group by customer_id, name, email
order by first_order_at limit 5
