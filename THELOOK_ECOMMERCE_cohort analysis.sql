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

*/
  

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


with ecommerce_cohort as
(select 
format_date('%Y-%m', t1.created_at) as Month,
format_date('%Y', t1.created_at) as Year,
t3.category as Product_category,
round(sum(t2.sale_price),2) as TPV,
count(t2.order_id) as TPO,
round(sum(t3.cost),2) as Total_cost
from bigquery-public-data.thelook_ecommerce.orders as t1
join bigquery-public-data.thelook_ecommerce.order_items as t2
on t1.order_id = t2.order_id
join bigquery-public-data.thelook_ecommerce.products as t3
on t2.id = t3.id 
group by 1,2,3
order by 1,2),

cte as(
select *,
lag(TPV) over(partition by Month order by Month) as next_rev,
lag(TPO) over(partition by Month order by Month) as next_order,
TPV-TPO as Total_profit
from ecommerce_cohort
)

select 
       Month,Year,Product_category,TPV,TPO,Total_cost,Total_profit,
       concat(round((next_rev - TPV)/TPV*100.0,2),"%") as Revenue_growth,
       concat(round((next_order - TPO)/TPO*100.0,2),"%") as Order_growth,
       round(Total_profit/Total_cost,2) as Profit_to_cost_ratio 
from cte
Order by Product_category, Year, Month

       
/***2. tỷ lệ số khách hàng quay lại ( RETENTION COHORT) ****/


with cte1 as 
(
select format_date('%Y-%m',first) as cohort_month,date, (extract(year from date)-extract(year from first))*12+
(extract(month from date)-extract(month from first))+1 as index,user_id
from
(
select user_id,created_at as date,
min(created_at) over(partition by user_id) as first
from  bigquery-public-data.thelook_ecommerce.order_items
where created_at between '2019-01-06'and '2021-01-31')),

cte2 as (
select cohort_month, index, count(distinct user_id) as number_user
from cte1
group by cohort_month,index),

--CUSTOMER COHORT-- 
customer_cohort as
(
select cohort_month,
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
select cohort_month,
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

with cte4 as 
(
select format_date('%Y-%m',first) as cohort_month,date, (extract(year from date)-extract(year from first))*12+
(extract(month from date)-extract(month from first))+1 as index,user_id
from
(
select user_id,created_at as date,
min(created_at) over(partition by user_id) as first
from  bigquery-public-data.thelook_ecommerce.order_items
where created_at between '2019-01-06'and '2020-01-31'))

select cohort_month, index, count(distinct user_id) as number_user
from cte 
group by cohort_month,index
order by cohort_month,index 


/*Retention cohort
- Số lượng người dùng mới hàng tháng tăng đều đặn → chiến dịch thu hút khách mới hiệu quả.
- Tuy nhiên: tỷ lệ khách quay lại (retention) trong 4 tháng đầu sau lần mua đầu tiên rất thấp (<10%) trong giai đoạn 2019–07/2023.
- Sau 10/2023, tỷ lệ retention tăng lên ~18% ở tháng tiếp theo.
→ Khách hàng trung thành của TheLook còn ít, doanh nghiệp vẫn chủ yếu dựa vào việc mua mới thay vì giữ chân.

Churn cohort
- Ngược lại, tỷ lệ rời bỏ (churn) cao trong những tháng đầu → cho thấy trải nghiệm khách hàng sau mua có thể chưa tốt hoặc thiếu chương trình giữ chân.

Kết luận & khuyến nghị
- Khách hàng mới: tăng đều qua từng năm → hiệu quả từ hoạt động quảng cáo và khuyến mãi.
- Khách hàng cũ: tỷ lệ quay lại thấp, chỉ cải thiện rõ rệt từ cuối 2023 → cần đầu tư nhiều hơn vào CRM, loyalty program, email marketing, voucher cá nhân hóa.
- Doanh thu & AOV: ổn định sau năm 2019 → có thể tập trung mở rộng tệp khách hàng hoặc upsell/cross-sell.
- Sản phẩm chủ lực: cần được duy trì, quảng bá mạnh hơn và tận dụng để kéo người dùng mới. */
