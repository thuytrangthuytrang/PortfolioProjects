/*****1) Doanh thu theo từng ProductLine, Year  và DealSize?
Output: PRODUCTLINE, YEAR_ID, DEALSIZE, REVENUE***/
select ProductLine, Year_id, DealSize,
		sum(sales) as Revenue
from public.sales_dataset_rfm_prj_clean
group by ProductLine, Year_id, DealSize
order by ProductLine, Year_id, DealSize


/***2) Đâu là tháng có bán tốt nhất mỗi năm?
Output: MONTH_ID, REVENUE, ORDER_NUMBER*****/

with cte as
(
select MONTH_ID,year_ID,ORDERNUMBER,REVENUE,
		row_number() over( partition by Year_id order by revenue desc,ORDERNUMBER desc) as ranking 
from 
(	
select MONTH_ID,year_ID,
		count(distinct ordernumber)  as ORDERNUMBER,
		sum(sales) as REVENUE
from public.sales_dataset_rfm_prj_clean
group by Year_id,MONTH_ID) as a 
)
select MONTH_ID,year_ID,ORDERNUMBER,REVENUE
from cte
where ranking=1
	

/*****3) Product line nào được bán nhiều ở tháng 11?
Output: MONTH_ID, REVENUE, ORDER_NUMBER	*****/

	
with cte as
(
	select *
	from 
	(
		select MONTH_ID, Year_id, Productline, 
				sum(sales) as revenue, count( ORDERNUMBER) as ORDER_NUMBER
		from public.sales_dataset_rfm_prj_clean 
		where MONTH_ID=11
		group by Productline, Year_id, MONTH_ID) as a
)

select MONTH_ID, Year_id,Productline,ORDER_NUMBER,revenue
from cte
order by Year_id, MONTH_ID, ORDER_NUMBER desc
	

/***4) Đâu là sản phẩm có doanh thu tốt nhất ở UK mỗi năm? 
Xếp hạng các các doanh thu đó theo từng năm.
Output: YEAR_ID, PRODUCTLINE,REVENUE, RANK**/


/***productline có doanh thu lớn nhất mỗi năm*/
with cte as
(
	select *, rank() over(partition by year_id order by revenue desc) as ranking
	from(
		select YEAR_ID, PRODUCTLINE, sum(sales) as REVENUE
		from public.sales_dataset_rfm_prj_clean
		where country='UK'
		group by PRODUCTLINE, YEAR_ID) as a)
		
/*** rank các product có doanh thu lơn nhất theo năm**/

select YEAR_ID,PRODUCTLINE,REVENUE,
		rank() over(order by REVENUE desc) as rank 
from cte
where ranking=1


/***5) Ai là khách hàng tốt nhất, phân tích dựa vào RFM 
(sử dụng lại bảng customer_segment ở buổi học 23)***/

/** tính RFM*/
with rfm as
	(
	select  customername, 
		current_date - max(orderdate) as R,
		count(distinct ordernumber) as F,
		sum(sales) as M 
	from public.sales_dataset_rfm_prj_clean
	group by customername),
/** tính điểm*/
rfm_score as
	(
	select customername, F,R,M,
		ntile(5) over(order by R desc) as R_score,
		ntile(5) over(order by F) as F_score,
		ntile(5) over(order by M) as M_score
	from rfm),
/* tạo tổ hợp điểm*/
cte as
(
	select customername, F,R,M,
		cast(R_score as varchar)||cast(F_score as varchar)||
		cast(M_score as varchar) as rfm_score
	from rfm_score)
/* khách hàng tốt nhất*/
select customername,r as time, f as order, m as money,segment
from(
	select a.customername,a.F,R,M,b.segment,
		row_number()over(order by m desc,f desc,r ) as ranking 
	from cte as a
	join public.segment_score as b
		on a.rfm_score=b.scores) as a
where ranking=1
