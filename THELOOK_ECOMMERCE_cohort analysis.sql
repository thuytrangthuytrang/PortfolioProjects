/*1.Thống kê tổng số lượng người mua và số lượng đơn hàng đã hoàn thành mỗi tháng ( Từ 1/2019-4/2022)
Output: month_year ( yyyy-mm) , total_user, total_orde*/

select format_date('%Y-%m', created_at) AS month,
       count(distinct user_id)  as total_user, count(id) as total_order
from bigquery-public-data.thelook_ecommerce.order_items
where status='Complete'and
      format_date('%Y-%m', created_at) between '2019-01' and '2022-04'
group by 1
order by 1

/*--> Insight: 
    - Nhìn chung số lượng người mua hàng và đơn hàng tiêu thụ đã hoàn thành tăng dần theo mỗi tháng và năm   
    - Giai đoạn 2019-tháng 1 2022: người mua hàng có xu hướng mua sắm nhiều hơn vào ba tháng cuối năm (10-12) và tháng 1 năm kế tiếp do nhu cầu mua sắm cuối/đầu năm tăng 
           và nhiều chương trình khuyến mãi/giảm giá cuối năm           
    - Giai đoạn bốn tháng đầu năm 2022: ghi nhận tỷ lệ lượng người mua tăng mạnh so với ba tháng cuối năm 2021, khả năng do TheLook triển khai chương trình khuyến mãi mới nhằm 
      kích cầu mua sắm các tháng đầu năm
    - Tháng 7 2021 ghi nhận lượng mua hàng tăng bất thường, trái ngược với lượng mua giảm sút so với cùng kì năm 2020, có thể do TheLook triển khai campaign đặc biệt cải thiện tình hình 
      doanh số cho riêng tháng 7.
*/


/*2. Giá trị đơn hàng trung bình (AOV) và số lượng khách hàng mỗi tháng*/

select format_date('%Y-%m', created_at) AS month_year,
       count(distinct user_id) as  distinct_users,
       sum(sale_price)/count( distinct id) as  average_order_value
 
from bigquery-public-data.thelook_ecommerce.order_items
where format_date('%Y-%m', created_at) between '2019-01' and '2022-04' 
group by 1
order by 1

/*--> Insight: - Giai đoạn năm 2019 do số lượng người dùng ít khiến giá trị đơn hàng trung bình qua các tháng có tỷ lệ biến động cao.
               - Giai đoạn từ cuối năm 2019 lượng người dùng ổn định trên 400 và nhìn chung tiếp tục tăng qua các tháng, giá trị đơn hàng trung bình qua các tháng ổn định ở mức ~80-90
 */



/*3.Tìm các khách hàng có trẻ tuổi nhất và lớn tuổi nhất theo từng giới tính ( Từ 1/2019-4/2022)*/


begin 

create temp table young_old as 
       ( select * from 
              (
               with cte as
              (
              select first_name, last_name,age,gender,
                    min(age) over(partition by gender) as min_age,
                    format_date('%Y-%m', created_at) as date
              from `bigquery-public-data.thelook_ecommerce.users`
              where format_date('%Y-%m', created_at) between '2019/01' and '2022/04'
              ),
              
              cte1 as
              (
              select first_name, last_name,age,gender,
                    max(age) over(partition by gender) as max_age,
                    format_date('%Y-%m', created_at) as date
              from `bigquery-public-data.thelook_ecommerce.users` 
              where format_date('%Y-%m', created_at) between '2019/01' and '2022/04'
              )
              
              select first_name, last_name,age,gender, 'youngest' as tag
              from cte
              where age= min_age
              
              union all
              
              select first_name, last_name,age,gender, 'oldest' as tag
              from cte1
              where age= max_age));

end;

/* số người nhỏ tuổi nhất */
select count(*)
from pristine-glass-406208._script259ccdc8a72adc5c1e9b44b5957a0ebd50cb9a98.young_old
where tag='youngest'

/*số người lớn tuổi nhất */
 
select count(*)
from pristine-glass-406208._scripta5b2dd14328521efa0282b23b6472a3b9d200b04.young_old
where tag='oldest'
 
  /*  
  --> Insight: trong giai đoạn Từ 1/2019-4/2022
      - Giới tính Female: lớn tuổi nhất là 70 tuổi (525 người người dùng); nhỏ tuổi nhất là 12 tuổi (569 người dùng)
      - Giới tính Male: lớn tuổi nhất là 70 tuổi (529 người người dùng); nhỏ tuổi nhất là 12 tuổi (546 người dùng)

/*
  

/*4. Top 5 sản phẩm mỗi tháng*/
with cte as
(
  
select format_date('%Y-%m',a.created_at) as month_year, a.product_id, b.name as product_name,
sum(a.sale_price) as  sales, sum (b.cost) as cost, sum(a.sale_price) - sum (b.cost) as profit

from  bigquery-public-data.thelook_ecommerce.order_items as a
join bigquery-public-data.thelook_ecommerce.products as b
  on a.product_id=b.id
group by 1,2,3  
order by 1,2
),


cte1 as (
select month_year, product_id, product_name, sales, cost, profit,
 dense_rank() over ( partition by month_year order by profit desc) as ranking 
from cte
)

select * from cte1 where ranking <=5
order by month_year 



/*5.Doanh thu tính đến thời điểm hiện tại trên mỗi danh mục
 trong 3 tháng qua ( giả sử ngày hiện tại là 15/4/2022)*/


select format_date('%Y-%m-%d',b.created_at) as datee,c.category as product_categories,
sum(b.sale_price) as revenue 
from bigquery-public-data.thelook_ecommerce.orders as a
join bigquery-public-data.thelook_ecommerce.order_items as b 
  on a.order_id=b.id
join bigquery-public-data.thelook_ecommerce.products as c 
  on b.product_id=c.id
where format_date('%Y-%m-%d',b.created_at)between '2022-01-15' and '2022-04-15'
group by 1,2   
order by 1, sum(b.sale_price) desc 


/************PART 2: Tạo retention cohort analysis**********/


with cte as
(select 
 format_date('%Y-%m', o.created_at) as Month
,format_date('%Y', o.created_at) as Year
,p.category as Product_category
,sum(sale_price) as TPV
,count(oi.order_id) as TPO
,sum(cost) as Total_cost
from bigquery-public-data.thelook_ecommerce.orders as t1
inner join bigquery-public-data.thelook_ecommerce.order_items as t2
on t1.order_id = t2.order_id
inner join bigquery-public-data.thelook_ecommerce.products as t3
on t2.id = t3.product_id 
group by 1,2,3
order by 1,2),

cte2 as(
select *
,lag(TPV) over(partition by Month order by Month) as next_rev
,lag(TPO) over(partition by Month order by Month) as next_order
,TPV-TPO as Total_profit
from cte
)

select 
       Month,Year,Product_category,TPV,TPO,Total_cost,Total_profit,
       concat(round((next_rev - TPV)/TPV*100.0,2),"%") as Revenue_growth,
       concat(round((next_order - TPO)/TPO*100.0,2),"%") as Order_growth,
       round(Total_profit/Total_cost,2) as Profit_to_cost_ratio 
from cte2


       
/***2. tỷ lệ số khách hàng quay lại ( RETENTION COHORT) ****/


with cte as 
(
select format_date('%Y-%m',first) as cohort_date,date, (extract(year from date)-extract(year from first))*12+
(extract(month from date)-extract(month from first))+1 as index,user_id
from
(
select user_id,created_at as date,
min(created_at) over(partition by user_id) as first
from  bigquery-public-data.thelook_ecommerce.order_items
where created_at between '2019-01-06'and '2021-01-31')),

cte1 as (
select cohort_date, index, count(distinct user_id) as number_user
from cte 
group by cohort_date,index),

--CUSTOMER COHORT-- 
customer_cohort as
(
select cohort_date,
sum (case when index = 1 then number_user else 0 end) as m1,
sum (case when index= 2 then number_user else 0 end) as m2,
sum (case when index= 3 then number_user else 0 end) as m3,
sum (case when index= 4 then number_user else 0 end) as m4
from cte1
group by cohort_date
order by cohort_date),

--RETENTION COHORT--
retention_cohort as
(
select cohort_date,
round(100.00*m1/m1,2) || '%' as m1,
round(100.00*m2/m1,2) || '%' as m2,
round(100.00*m3/m1,2) || '%' as m3,
round(100.00*m4/m1,2) || '%' as m4,
from customer_cohort)

--CHURN COHORT--
Select cohort_month,
(100.00 - round(100.00* m1/m1,2)) || '%' as m1,
(100.00 - round(100.00* m2/m1,2)) || '%' as m2,
(100.00 - round(100.00* m3/m1,2)) || '%' as m3,
(100.00 - round(100.00* m4/m1,2))|| '%' as m4
from customer_cohort


/*Visualize số khách hàng quay lại trong 1 năm từ 1/2019-1/2020*/

with cte as 
(
select format_date('%Y-%m',first) as cohort_date,date, (extract(year from date)-extract(year from first))*12+
(extract(month from date)-extract(month from first))+1 as index,user_id
from
(
select user_id,created_at as date,
min(created_at) over(partition by user_id) as first
from  bigquery-public-data.thelook_ecommerce.order_items
where created_at between '2019-01-06'and '2020-01-31'))

select cohort_date, index, count(distinct user_id) as number_user
from cte 
group by cohort_date,index
order by cohort_date,index 

/*
Nhìn chung hằng tháng TheLook ghi nhận số lượng người dùng mới tăng dần đều, thể hiện chiến dịch quảng cáo tiếp cận người dùng
mới có hiệu quả.
Tuy nhiên trong giai đoạn 4 tháng đầu tính từ lần mua hàng/sử dụng trang thương mại điện tử TheLook, tỷ lệ người dùng cũ
quay lại sử dụng trong tháng kế tiếp khá thấp: dao động dưới 10% trong giai đoạn từ 2019-01 đến 2023-07 và tăng lên mức 
trên 10% trong những tháng còn lại của năm 2023, trong đó cao nhất là tháng đầu tiên sau 2023-10 với 18.28%.
 --> Tỷ lệ khách hàng trung thành thấp, TheLook nên xem xét cách quảng bá để thiếp lập và tiếp cận nhóm khách hàng trung thành
nhằm tăng doanh thu từ nhóm này và tiết kiệm các chi phí marketing
*/
