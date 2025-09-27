/* PHẦN 1: CLEANING*/

/*****1. Chuyển đổi kiểu dữ liệu phù hợp cho các trường*/

SET datestyle = 'iso,mdy';  
ALTER TABLE sales_dataset_rfm_prj
ALTER COLUMN orderdate TYPE date USING (TRIM(orderdate):: date),
ALTER COLUMN quantityordered TYPE numeric USING(trim(quantityordered)::numeric),
ALTER COLUMN priceeach TYPE decimal USING(trim(priceeach)::decimal),
ALTER COLUMN orderlinenumber TYPE numeric USING(trim(orderlinenumber)::numeric),
ALTER COLUMN sales TYPE numeric USING(trim(sales)::float),
ALTER COLUMN ordernumber TYPE numeric USING(trim(ordernumber)::numeric),
ALTER COLUMN msrp TYPE numeric  USING(trim(msrp)::numeric)


/****2. Check NULL*/
ALTER TABLE sales_dataset_rfm_prj
ADD CHECK ( ORDERNUMBER IS NOT NULL),
ADD CHECK ( QUANTITYORDERED IS NOT NULL),
ADD CHECK ( PRICEEACH IS NOT NULL),                      
ADD CHECK ( ORDERLINENUMBER IS NOT NULL),
ADD CHECK ( SALES IS NOT NULL),
ADD CHECK ( ORDERDATE  IS NOT NULL)



/****3. Thêm cột CONTACTLASTNAME, CONTACTFIRSTNAME */
ALTER TABLE sales_dataset_rfm_prj
ADD COLUMN CONTACTFIRSTNAME VARCHAR(255),
ADD COLUMN CONTACTLASTNAME VARCHAR(255)
	

UPDATE sales_dataset_rfm_prj
SET
CONTACTFIRSTNAME =UPPER (LEFT(CONTACTFULLNAME,1)) || 
RIGHT(LEFT(CONTACTFULLNAME,POSITION('-' IN CONTACTFULLNAME)-1),
LENGTH(LEFT(CONTACTFULLNAME,POSITION('-' IN CONTACTFULLNAME)-1))-1), 
	

CONTACTLASTNAME = UPPER (LEFT(right(CONTACTFULLNAME, LENGTH(CONTACTFULLNAME)-
LENGTH(LEFT(CONTACTFULLNAME,POSITION('-' IN CONTACTFULLNAME)))),1))
|| right(right(CONTACTFULLNAME, LENGTH(CONTACTFULLNAME)-
LENGTH(LEFT(CONTACTFULLNAME,POSITION('-' IN CONTACTFULLNAME)))),
LENGTH(right(CONTACTFULLNAME, LENGTH(CONTACTFULLNAME)-
LENGTH(LEFT(CONTACTFULLNAME,POSITION('-' IN CONTACTFULLNAME)))-1)))



/**********Thêm cột QTR_ID, MONTH_ID, YEAR_ID lần lượt là Qúy,
tháng, năm được lấy ra từ ORDERDATE */

ALTER TABLE sales_dataset_rfm_prj
ADD COLUMN QTR_ID int,
ADD COLUMN MONTH_ID int,
ADD COLUMN YEAR_ID int


UPDATE sales_dataset_rfm_prj
SET 	QTR_ID=EXTRACT( QUARTER FROM ORDERDATE),
	MONTH_ID=EXTRACT( MONTH FROM ORDERDATE),
	YEAR_ID=EXTRACT( YEAR FROM ORDERDATE)
	

	
/******5. outlier*////
/* box plot*/

WITH cte AS
	(SELECT Q1-1.5*IQR AS min_value, Q3+1.5*IQR AS max_value
	FROM
		(SELECT 
		percentile_cont (0.25) WITHIN GROUP (ORDER BY QUANTITYORDERED ) as Q1,
		percentile_cont (0.75) WITHIN GROUP (ORDER BY QUANTITYORDERED ) as Q3,
		percentile_cont (0.75) WITHIN GROUP (ORDER BY QUANTITYORDERED ) -
		percentile_cont (0.25) WITHIN GROUP (ORDER BY QUANTITYORDERED ) as IQR
		FROM sales_dataset_rfm_prj) as bbb)
 
select *
from sales_dataset_rfm_prj
where 
QUANTITYORDERED < (select min_value from cte)
 or
QUANTITYORDERED > (select max_value from cte)
  
  /* Z-CORE*/
  
WITH cte AS
	(SELECT  QUANTITYORDERED, 
		(select avg(QUANTITYORDERED)
  		from sales_dataset_rfm_prj) as av ,
 		(select stddev(QUANTITYORDERED)
		from sales_dataset_rfm_prj) as stddev 
	FROM sales_dataset_rfm_prj),
	 
cte1 AS
	(select QUANTITYORDERED,(QUANTITYORDERED- av)/stddev as z_score
	from cte
	where abs(QUANTITYORDERED- av)/stddev) > 2 )

/* xử lý outlier*/	
DELETE from sales_dataset_rfm_prj
WHERE QUANTITYORDERED in (select QUANTITYORDERED from cte1)


/******6. Lưu vào bảng mới tên là SALES_DATASET_RFM_PRJ_CLEAN*/

CREATE TABLE SALES_DATASET_RFM_PRJ_CLEAN AS
(SELECT * FROM sales_dataset_rfm_prj )


/* PHẦN 2: RFM ANALYST*/

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
(sử dụng lại bảng customer_segment)***/

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
