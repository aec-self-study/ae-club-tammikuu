{{ config(
    materialized='table'
 ) }}

 select
    product_id,
    price,
    created_at, 
    ended_at
from {{ref('stg_coffee_shop__product_prices')  }} as product_prices
