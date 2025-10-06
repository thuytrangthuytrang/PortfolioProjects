--1.Thống kê tổng số lượng người mua và số lượng đơn hàng, giá trị đơn hàng trung bình đã hoàn thành mỗi tháng ( Từ 1/2024-9/2025).--

select 
 format_date('%Y-%m', t1.created_at) AS month,
 count(distinct t1.user_id)  as total_user, count(t1.id) as total_order,
 round(sum(t1.sale_price * t2.num_of_item),2) as revenue,
 round(sum (t3.cost),2) as cost, 
 round(sum(t1.sale_price) - sum (t3.cost),2) as profit,
 round(sum(t1.sale_price * t2.num_of_item)/count( distinct t1.id),2) as  average_order_value
from bigquery-public-data.thelook_ecommerce.order_items as t1
  join bigquery-public-data.thelook_ecommerce.orders as t2
      on t1.order_id = t2.order_id
  join bigquery-public-data.thelook_ecommerce.products as t3
  on t1.product_id=t3.id
where 
 t1.status='Complete'and
 format_date('%Y-%m', t1.created_at) between '2024-01' and '2025-09'
group by month
order by month

--2. Thống kê khách hàng theo độ tuổi ( Từ 1/2024-9/2025).

select
  case
    when t2.age < 12 then 'Kids'
    when t2.age between 12 and 20 then 'Teenagers'
    when t2.age between 20 and 30 then 'Young Adults'
    when t2.age between 30 and 50 then 'Adults'
    when t2.age > 50 then 'Elderly'
    end as age_group,
  count(distinct t1.user_id) as total_customer
from bigquery-public-data.thelook_ecommerce.order_items as t1
join bigquery-public-data.thelook_ecommerce.users as t2
on t1.user_id = t2.id
group by age_group
order by total_customer desc;

--Thống kê khách hàng theo giới tính ( Từ 1/2024-9/2025).

select 
  t2.gender,
  count(t1.user_id) as customer,
  round(sum(t1.sale_price * t2.num_of_item), 2) as revenue,
  sum(t2.num_of_item) as quantity
from bigquery-public-data.thelook_ecommerce.order_items as t1
	join bigquery-public-data.thelook_ecommerce.orders as t2
		on t1.user_id = t2.user_id
where t1.status not in ('cancelled', 'returned')
group by gender
order by revenue desc;

-- Thống kê khách hàng theo quốc gia ( Từ 1/2024-9/2025).

with customers as (
  select
    distinct t1.user_id,
    round(sum(t1.sale_price * t2.num_of_item), 2) as revenue,
    sum(case when t4.gender = 'M' then 1 else null end) as male,
    sum(case when t4.gender = 'F' then 1 else null end) as female,
    t4.country as country
  from bigquery-public-data.thelook_ecommerce.order_items as t1
	  join bigquery-public-data.thelook_ecommerce.orders as t2
		on t1.user_id = t2.user_id
    join bigquery-public-data.thelook_ecommerce.users as t4
      on t1.user_id = t4.id
  where 
    t1.status not in ('cancelled', 'returned') and
    format_date('%Y-%m', t1.created_at) between '2024-01' and '2025-09'
  group by t1.user_id, t4.country
    )

select
  country,
  count(distinct user_id) as total_customers,
  revenue,
  count(female) as female,
  count(male) as male
from customers
group by country
order by total_customers desc;

--5.Top 10 doanh thu theo danh mục sản phẩm ( Từ 1/2024-9/2025).

select
  category as product_category,
  round(sum(sale_price * num_of_item), 2) as revenue,
  sum(num_of_item) as quantity
from bigquery-public-data.thelook_ecommerce.order_items t1
inner join bigquery-public-data.thelook_ecommerce.orders t2
on t1.order_id = t2.order_id
inner join bigquery-public-data.thelook_ecommerce.products t3
on t1.product_id = t3.id
where t1.status not in ('cancelled', 'returned') and format_date('%Y-%m', t1.created_at) between '2024-01' and '2025-09'
group by category
order by revenue desc
limit 10

--6. Cohort Analysis ( Từ 1/2024-9/2025).
with cte1 as 
	(
	select 
		format_date('%Y-%m',first) as cohort_month,date, 
		(extract(year from date)-extract(year from first))*12+
		(extract(month from date)-extract(month from first))+1 as index,user_id
	from
		(
		select 
			user_id,created_at as date,
			min(created_at) over(partition by user_id) as first
		from  bigquery-public-data.thelook_ecommerce.order_items
		where created_at between '2024-01-01'and '2025-09-30')),

cte2 as 
(
	select 
		cohort_month, index, count(distinct user_id) as number_user
	from cte1
	group by cohort_month,index),

--CUSTOMER COHORT-- 
customer_cohort as
(
	select 
		cohort_month,
		sum (case when index = 1 then number_user else 0 end) as m1,
		sum (case when index= 2 then number_user else 0 end) as m2,
		sum (case when index= 3 then number_user else 0 end) as m3,
		sum (case when index= 4 then number_user else 0 end) as m4
	from cte2
	group by cohort_month
	order by cohort_month),

--RETENTION COHORT--
retention_cohort as
(
	select 
		cohort_month,
		round(100.00*m1/m1,2) || '%' as m1,
		round(100.00*m2/m1,2) || '%' as m2,
		round(100.00*m3/m1,2) || '%' as m3,
		round(100.00*m4/m1,2) || '%' as m4,
	from customer_cohort)

--CHURN COHORT--
Select 
	cohort_month,
	(100.00 - round(100.00* m1/m1,2)) || '%' as m1,
	(100.00 - round(100.00* m2/m1,2)) || '%' as m2,
	(100.00 - round(100.00* m3/m1,2)) || '%' as m3,
	(100.00 - round(100.00* m4/m1,2))|| '%' as m4
from customer_cohort
