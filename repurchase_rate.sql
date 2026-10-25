
with 

rk as 
(
SELECT 
order_id, user_id, cast(transaction_date as date) as transaction_date, gross_amount, 
row_number() over (partition by user_id order by transaction_date asc) as order_rk
FROM `orders` 
) 

, user as (
select user_id, 
min(case when order_rk = 1 then transaction_date end) as first_order_date, 
min(case when order_rk = 2 then transaction_date end) as second_order_date
from rk 
group by user_id 
)

, user2 as (

select *, date_diff(second_order_date, first_order_date, DAY) as days_to_second_order
from user
) 


select 
date_trunc(first_order_date, month) as first_order_month, 
count(distinct user_id) as customers, 
count(distinct case when days_to_second_order <= 30 then user_id end) as customers_repurchase_30d, 
count(distinct case when days_to_second_order <= 90 then user_id end) as customers_repurchase_90d, 
count(distinct case when days_to_second_order <= 30 then user_id end)/ 
count(distinct user_id) repurchase_rate_30, 
count(distinct case when days_to_second_order <= 90 then user_id end)/ 
count(distinct user_id) repurchase_rate_90 
from user2 
group by 1 order by 1 

