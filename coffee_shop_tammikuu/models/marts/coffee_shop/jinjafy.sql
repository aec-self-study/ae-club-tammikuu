select
  date_trunc(date_of_order, month) as date_month,
  sum(case when category = 'coffee beans' then price_for_week end) as coffee_beans_amount,
  sum(case when category = 'merch' then price_for_week end) as merch_amount,
  sum(case when category = 'brewing supplies' then price_for_week end) as brewing_supplies_amount
-- you may have to `ref` a different model here, depending on what you've built previously
from {{ ref('weekly_revenues') }}
group by 1