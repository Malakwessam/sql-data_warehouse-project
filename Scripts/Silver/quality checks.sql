
/*
=========================================================================================
DATA QUALITY VALIDATION & CLEANING SCRIPT  
-----------------------------------------------------------------------------------------
This script performs a comprehensive set of data quality checks across multiple CRM and 
ERP source tables in both the Bronze (raw) and Silver (cleaned) layers. It identifies 
and validates issues such as:

1. Duplicates and missing primary keys (e.g., customer IDs).
2. Unwanted leading/trailing spaces and inconsistent text formatting.
3. Standardization issues in attributes such as gender, marital status, product categories, etc.
4. Referential integrity mismatches between CRM and ERP systems.
5. Invalid or illogical date values, including incorrect formats, impossible ranges, and 
   non-sequential order dates.
6. Product and sales data validations, including:
      - Negative or zero prices, costs, quantities, and computed sales amounts.
      - Incorrect product category mappings.
      - Product lifecycle date conflicts.
7. Cleaning logic examples for standardizing fields and recalculating invalid metrics.

Overall, this script ensures that raw operational data is trustworthy, consistent, and 
ready for Silver-layer transformation, analytical modeling, and downstream reporting.
=========================================================================================
*/

select * from bronze.crm_prd_info;
--check for nulls and duplicates
--expectation:row num =1
select cst_id , count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1;  --HAVING filters groups after aggregation; WHERE filters rows before aggregation.

--solved
--cte
with duplicates as(
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as ranks
from silver.crm_cust_info)
select *
from duplicates
where ranks >1;

--subquery
select *
from(
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as ranks
from bronze.crm_cust_info
where cst_id is not null) t
where ranks =1;

-- checking for unwanted spaces
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

--check for consistincy(standralization)
select distinct cst_material_status
from silver.crm_cust_info


select distinct cst_gndr
from bronze.crm_cust_info


select *
from silver.crm_cust_info

--filterig unmatched data
select 
prd_id,
replace(substring(prd_key,1,5),'-','_' )as cat_id

from bronze.crm_prd_info
where replace(substring(prd_key,1,5),'-','_' ) not in 
(select id 
from bronze.erp_PX_CAT_G1V2)

-- checking for unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)



--cheking cost -ve or 0
select prd_cost
from bronze.crm_prd_info
where prd_cost <= 0 or prd_cost is null

select distinct prd_line
from bronze.crm_prd_info;


select 
prd_key,
prd_start_dt ,
prd_end_dt
from bronze.crm_prd_info
order by prd_key

--checking invalid date orders
-- solution = end date is the start date of th enext row(lead)
select 
prd_key,
prd_start_dt ,
prd_end_dt,
prd_cost
from silver.crm_prd_info
where prd_start_dt>prd_end_dt
order by prd_key


select *
from silver.crm_prd_info

---bronze.crm_sales_details
select * from bronze.crm_sales_details

select sls_order_num ,count(sls_order_num)
from bronze.crm_sales_details
group by  sls_order_num

select *
from bronze.crm_sales_details
where sls_order_num='SO55367'  --aame order nnum for diff products

select *
from bronze.crm_sales_details
where sls_order_num!=trim(sls_order_num)


select *
from bronze.crm_sales_details
where sls_cust_id not in(select cst_id from [silver].[crm_cust_info])


--check for invalid dates  --int -->varchar -->date
select
nullif(sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <=0   --check for zeros/-ve  bec it cant be converted to date (casted)
or len(sls_order_dt)!=8   --lenght must be 8 
or sls_order_dt<19900101
or sls_order_dt>20300101   -- validating range

select  --invalid date orders
sls_due_dt,sls_order_dt,sls_ship_date
from silver.crm_sales_details
where  sls_order_dt !<  sls_ship_date     
and sls_ship_date !< sls_due_dt  


--sales=quantity*price   and not negative
select
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales!= sls_quantity* sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or  sls_sales <=0  or sls_quantity <=0  or sls_price <=0 
order by sls_sales,
sls_quantity,
sls_price

select
case when sls_sales is null or sls_sales <=0 or sls_sales!= sls_quantity*ABS(sls_price)
	then  sls_quantity*ABS(sls_price)
	else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price <0 then abs(sls_price)
	when sls_price is null then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales!= sls_quantity* sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or  sls_sales <=0  or sls_quantity <=0  or sls_price <=0 
order by sls_sales,
sls_quantity,
sls_price

select *
from silver.crm_sales_details


--[bronze].[erp_CUST_AZ12]

 select 
 case when CID like 'NAS%' then substring(CID,4,len(CID))
		else CID
end as CID
 from bronze.erp_CUST_AZ12
 where  case when CID like 'NAS%' then substring(CID,4,len(CID))
		else CID
end  not in(select cst_key from silver.crm_cust_info)

select gen from bronze.erp_CUST_AZ12 
where gen !=trim(gen)


select distinct gen from silver.erp_CUST_AZ12 




-- --silver.erp_LOC_A101

select distinct cntry
from silver.erp_LOC_A101
order by 1

select CID
FROM bronze.erp_LOC_A101

select cst_key
from [silver].[crm_cust_info]


select *
from silver.erp_LOC_A101



--bronze.erp_PX_CAT_G1V2
 
 --checking for unwanted spaces
 select 
 ID,
 CAT,
 SUBCAT,
 MAINTENANCE
 from silver.erp_PX_CAT_G1V2
 where  cat !=trim(cat) or subcat !=trim(subcat) or  MAINTENANCE!=trim( MAINTENANCE)

 select distinct subcat
 from bronze.erp_PX_CAT_G1V2
 order by 1