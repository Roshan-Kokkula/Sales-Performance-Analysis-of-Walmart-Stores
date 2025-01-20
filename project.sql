/*  task 1*/ 

with sales_data_by_month as 
(select branch,round(sum(total)) as total_revenue,month(datee) as Mnth,sum(quantity) as total_sales,
lag(sum(quantity)) over(partition by branch order by branch,month(datee))  as prvs_mnth_sales
from walmartsales
group by branch,mnth
order by
branch,mnth)

select *,(((total_sales-prvs_mnth_sales)/prvs_mnth_sales)*100)  as growth_rate 
from sales_data_by_month 
order by growth_rate desc;



/* task 2 */

select product_line ,branch,avg(gross_income - cogs) as profit_margin, 
dense_rank() over ( partition by branch order by avg(gross_income - cogs) desc) as rank_
from walmartsales
group by branch,product_line
order by branch,profit_margin desc;



/* task 3 */
select *,
case
when total <300 then 'Low Spenders'
when total between 301 and 800 then 'Medium Spenders'
else 'High Spenders' end as Spending_class
from walmartsales;

/* task  4 */

create table avg_sales 
select round(avg(Quantity),1) as avg_sales_On_this_product_line,Product_line
from walmartsales
group by Product_line
order by Product_line;

select * from walmartsales w inner join avg_sales a on a.Product_line = w.product_line
where abs(quantity - avg_sales_On_this_product_line) > 0.75 * avg_sales_On_this_product_line; 

select customer_id,branch,city,quantity,avg_sales_On_this_product_line, datee from walmartsales w inner join avg_sales a on a.Product_line = w.product_line
where abs(quantity - avg_sales_On_this_product_line) > 0.75 * avg_sales_On_this_product_line; 

select * from avg_sales; 


/* task 5*/ 

create view city_wis_payment_method as 
select city,payment,count(payment) as total_transactions
from walmartsales
group by city,payment
order by city,total_transactions desc;
/* to see only the top payment method from all cities */
select * from city_wis_payment_method
where total_transactions in (select max(total_transactions) from city_wis_payment_method group by city )
order by total_transactions desc;

/* to see all types of payments distribution in all cities*/

select * from city_wis_payment_method order by city,total_transactions desc;

/* task 6 */ 

select date_,STR_TO_DATE(date_, '%d-%m-%Y') as Date__
from walmartsales;
 
alter table walmartsales
add Datee date;
 
update walmartsales
set datee = STR_TO_DATE(date_, '%d-%m-%Y');
 
select gender,round(sum(total)) as total_revenue,sum(quantity) as total_units , month(datee) as mnth,count(distinct datee) as No_of_days
from walmartsales
group by gender,mnth
order by  mnth asc;

 /* task 7 */ 
 
with Cus_type_on_prdt_line as
(select Customer_type,round(sum(total)) as total_revenue,sum(Quantity) as total_sales,Product_line,
dense_rank() over(partition by customer_type order by sum(total) desc)  as rank_
from walmartsales
group by Customer_type,Product_line
order by Customer_type,rank_,Product_line,total_revenue desc)
/* all product lines arranged by total revenue and ranked to show the order */
select * from Cus_type_on_prdt_line;

/* only top  3 product lines for each memeber type */
select * from Cus_type_on_prdt_line where rank_ in (1,2,3) order by total_revenue desc ;


/* task 8 */

select Customer_ID,count( Customer_ID) as num_of_transaction,round(sum(total)) as purchase_value,Branch  
from walmartsales
where datee between '2019-03-01' and '2019-03-30'
group by Customer_ID,Branch
order by branch,num_of_transaction desc;


/* task 9 */

select Customer_ID,round(sum(total)) as total_revenue,sum(Quantity) as total_sales,branch
from walmartsales
group by Customer_ID,City,branch
order by total_revenue desc
limit 5;

/* Task 10 */ 

alter table walmartsales
add column Day_of_week varchar(25);

update walmartsales
set Day_of_week = dayname(datee);

select sum(quantity) as total_sales_of_day,day_of_week 
from walmartsales
group by day_of_week
order by total_sales_of_day desc;

/* overall Analysis for better understanding of data*/
/* overall month wise total sales and revenue and transaction irrespective of city and branch */
select monthname(datee)as Mnth,sum(quantity) as Units_sold,round(sum(total)) as Revenue_generated,count( Customer_ID) as Number_of_Transactions   
from walmartsales
group by monthname(datee);

/* distribution of customer types  irrespective of city and branch */
select Customer_type,round(sum(total))  as Revenue_generated,sum(Quantity) as Units_sold
from walmartsales group by Customer_type;

/* overall most profitable product line irrespective of city and branch*/
select Product_line,round(sum(total))  as Revenue_generated,sum(Quantity) as Units_sold
from walmartsales group by Product_line;

/* overall most profitable product line irrespective of branch*/
select city,round(sum(total)) as Revenue_generated,sum(Quantity) as Units_sold
from walmartsales group by city;

/* overall most profitable payment method irrespective of branch and city*/
select payment,round(sum(total)) as Revenue_generated,count(Customer_ID) as Number_of_transactions
from walmartsales group by payment;

