WITH 

base AS (
  -- 1. Create a master base of all Date and Country combinations
  -- This ensures zero data gaps even if a specific country has no actions on a given day
  SELECT 
    report_date,
    country
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2025-12-31', INTERVAL 1 DAY)) AS report_date -- use the date range you want
  CROSS JOIN 
    UNNEST(['Germany', 'France', 'UK', 'Spain']) AS country -- use your countries
),

orders_aggregated AS (
  -- 2. Aggregate Order metrics by Date and Country
  SELECT
    order_date,
    country,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(amount) AS total_revenue
  FROM 
    `dwh.orders`
  GROUP BY 
    1, 2
),

logins_aggregated AS (
  -- 3. Aggregate Traffic metrics by Date and Country
  SELECT
    login_date,
    country,
    COUNT(DISTINCT user_id) AS unique_active_users
  FROM 
    `dwh.logins`
  GROUP BY 
    1, 2
),

calls_aggregated AS (
  -- 4. Aggregate Support metrics by Date and Country
  SELECT
    call_date,
    country,
    COUNT(DISTINCT call_id) AS total_customer_calls
  FROM 
     `dwh.customer_calls`
  GROUP BY 
    1, 2
)

-- 5. Final Merge: Bring everything together seamlessly onto the backbone
SELECT
  b.report_date,
  b.country,
  
  -- Handle null values safely so your BI tools render zero instead of blank charts
  COALESCE(o.total_orders, 0) AS total_orders,
  COALESCE(o.total_revenue, 0.0) AS total_revenue,
  COALESCE(l.unique_active_users, 0) AS unique_active_users,
  COALESCE(c.total_customer_calls, 0) AS total_customer_calls
FROM 
  base b
LEFT JOIN 
  orders_aggregated o ON b.report_date = o.order_date AND b.country = o.country
LEFT JOIN 
  logins_aggregated l ON b.report_date = l.login_date AND b.country = l.country
LEFT JOIN 
  calls_aggregated c ON b.report_date = c.call_date AND b.country = c.country
ORDER BY 
  country ASC,  report_date asc
