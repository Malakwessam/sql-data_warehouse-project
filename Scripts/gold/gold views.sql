/*
===============================================================================
 Create Gold Views:
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- Create Dimension: gold.dim_customers
DROP VIEW IF EXISTS gold.dim_customers;
go
create view gold.dim_customers as
select 
ROW_NUMBER() over(order by A.cst_id) as customer_surrogate_key,
A.cst_id as customer_id,
A.cst_key as customer_number,
A.cst_firstname as first_name,
A.cst_lastname as last_name,
C.CNTRY as country,
A.cst_material_status as material_status,
case
	when A.cst_gndr!='n/a' then A.cst_gndr  --cst_gender is the master
	else coalesce(B.GEN,'n/a' )--return the first non‑NULL value from a list of expressions.
end as gender,
B.BDATE as birthdate,
A.cst_create_date as create_date
from silver.crm_cust_info as A
left join silver.erp_CUST_AZ12 AS B
on A.cst_key = B.CID
left join silver.erp_LOC_A101 as C
on A.cst_key = C.CID


go

-- Create Dimension: gold.dim_products
DROP VIEW IF EXISTS gold.dim_products;
go
create view gold.dim_products as
select 
row_number() over(order by prd_start_dt,p1.prd_key) as product_surrogate_key,
p1.prd_id as product_id,
p1.prd_key as product_number,
p1.prd_nm as product_name,
p1.prd_line as product_line,
p1.cat_id as cateogry_id,
p2.cat as category,
p2.subcat as sub_category,
p1.prd_cost as cost,
p2.MAINTENANCE as maintenance,
p1.prd_start_dt
from silver.crm_prd_info as p1
left join silver.erp_PX_CAT_G1V2 as p2
on p1.cat_id=p2.id 
where prd_end_dt is null

go

-- Create Fact Table: gold.fact_sales
DROP view IF EXISTS gold.fact_sales;
go
create view  gold.fact_sales as
select
	sls.sls_order_num as order_number,
	product_surrogate_key,
	customer_surrogate_key,
	sls.sls_order_dt as order_date ,
	sls.sls_ship_date as ship_date,
	sls.sls_due_dt as due_date,
	sls.sls_sales as sales,
	sls.sls_quantity as quantity ,
	sls.sls_price as price
from silver.crm_sales_details as sls
left join gold.dim_products as dim1
on sls.sls_prd_key=product_number
left join gold.dim_customers as dim2
on sls.sls_cust_id=customer_id
go

select * 
from gold.fact_sales as f
left join gold.dim_products as p
on f.product_surrogate_key=p.product_surrogate_key
where f.product_surrogate_key is null