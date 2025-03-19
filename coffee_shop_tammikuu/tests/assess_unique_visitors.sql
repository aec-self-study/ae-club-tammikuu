-- for each customer_id there can be only one unique_visitor_id

select 
    pageview_id
from 
    {{ ref('unique_visitors') }}
where
    customer_id is not null
group by 1
having count(distinct unique_visitor_id) > 1