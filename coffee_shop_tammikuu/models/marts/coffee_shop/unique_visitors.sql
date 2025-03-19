{{ config(
    materialized='view'
 ) }}

 with unique_visitor as (
    select 
        pageview_id,
        min(pageviews.visitor_id) over (partition by pageviews.customer_id) as unique_visitor_id
    from 
        {{ ref('stg_web_tracking__pageviews') }} as pageviews
    where 
        customer_id is not null

 )

 select
    pageviews.pageview_id,
    customer_id,
    visitor_id,
    unique_visitor_id,
    device_type,
    page_visited, 
    visit_timestamp
from 
    {{ ref('stg_web_tracking__pageviews') }} as pageviews 
left join unique_visitor on pageviews.pageview_id = unique_visitor.pageview_id