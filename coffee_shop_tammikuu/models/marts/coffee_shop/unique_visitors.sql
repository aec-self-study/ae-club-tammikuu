{{ config(
    materialized='view'
 ) }}

 with unique_visitors as (
    select 
        pageview_id,
        min(pageviews.visitor_id) over (partition by pageviews.customer_id) as unique_visitor_id
    from 
        {{ ref('stg_web_tracking__pageviews') }} as pageviews
    where 
        pageviews.customer_id is not null

 ), 

 visit_sessions as (
    select 
        pageviews.pageview_id,
        min(pageviews.pageview_id) over (partition by pageviews.customer_id order by unix_seconds(pageviews.visit_timestamp) range between 1800 preceding and 1800 following) as session_id,
        min(pageviews.visit_timestamp) over (partition by pageviews.customer_id order by unix_seconds(pageviews.visit_timestamp) range between 1800 preceding and 1800 following) as session_start_time,
        max(pageviews.visit_timestamp) over (partition by pageviews.customer_id order by unix_seconds(pageviews.visit_timestamp) range between 1800 preceding and 1800 following) as session_end_time,
    from 
        {{ ref('stg_web_tracking__pageviews') }} as pageviews
    where
        pageviews.customer_id is not null
 )      

 select
    pageviews.pageview_id,
    pageviews.customer_id,
    pageviews.visitor_id,
    unique_visitors.unique_visitor_id,
    pageviews.device_type,
    pageviews.page_visited, 
    pageviews.visit_timestamp,
    visit_sessions.session_id, 
    visit_sessions.session_start_time,
    visit_sessions.session_end_time
from 
    {{ ref('stg_web_tracking__pageviews') }} as pageviews 
left join unique_visitors 
    on pageviews.pageview_id = unique_visitors.pageview_id
left join visit_sessions
    on pageviews.pageview_id = visit_sessions.pageview_id