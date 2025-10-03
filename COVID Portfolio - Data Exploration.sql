WITH first_purchase AS (
  SELECT 
    user_id,
    MIN(DATE(created_at)) AS first_order_date
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  GROUP BY user_id
),
cohort AS (
  SELECT
    f.user_id,
    DATE_TRUNC(f.first_order_date, MONTH) AS cohort_month,
    DATE_TRUNC(o.created_at, MONTH) AS active_month
  FROM first_purchase f
  JOIN `bigquery-public-data.thelook_ecommerce.orders` o
    ON f.user_id = o.user_id
)
SELECT
  cohort_month,
  active_month,
  COUNT(DISTINCT user_id) AS active_users,
  EXTRACT(YEAR FROM active_month)*12 + EXTRACT(MONTH FROM active_month)
    - (EXTRACT(YEAR FROM cohort_month)*12 + EXTRACT(MONTH FROM cohort_month)) + 1 
    AS cohort_index
FROM cohort
GROUP BY cohort_month, active_month
ORDER BY cohort_month, cohort_index;
