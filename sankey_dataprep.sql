

###########################################################################################


##               HOW TO CREATE DATABASIS FOR SANKEY CHART (LOOKER STUDIO)


## Dataset:   The public ecommerce thelook dataset,  `bigquery-public-data.thelook_ecommerce.events`


###########################################################################################


with 

# Make a rank variable, and use lead function to get the next page visit
base as (
  SELECT session_id, created_at, 
  row_number()  over (partition by session_id order by created_at asc) as rk, 
  event_type , 
  lead(event_type) over (partition by session_id order by created_at asc) as next_event_type 
  FROM `bigquery-public-data.thelook_ecommerce.events` 
)

# Make source and destination event unique, so that we handle each step separate
, base2 as (
  select *, 
  concat(rk, "_", event_type) as source_event, 
  concat(rk+1, "_", next_event_type) as destination_event, 
  min(case when event_type = 'purchase' then created_at end) over (partition by session_id) as first_purchase_created -- no purchase -> null valyue
  from base 
  order by session_id, created_at
--LIMIT 1000
)

# Make the final aggregation 
select source_event, 
case when destination_event like '%purchase%' then 'purchase' else destination_event end as destination_event, -- consolidate all purchase destination events into one
count(distinct session_id) as sessions
from base2
where 1=1 
and created_at < coalesce(first_purchase_created, current_timestamp) -- in case a session has a purchase event, remove everything that happens thereafter
group by 1,2 
order by 1,2 
