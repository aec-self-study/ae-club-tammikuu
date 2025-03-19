{{ config(
    materialized='table'
 ) }}

with source as (
    select * from {{ source('web_tracking', 'pageviews') }}
),

renamed as (
    select
        id as pageview_id,
        customer_id,
        visitor_id,
        device_type,
        page as page_visited, 
        timestamp as visit_timestamp
    from source    
)

select * from renamed