
--check for duplicates customers after joining tables
--COUNT(*) returns the total number of rows in a table 
select cst_id,count(*) from
(select 
A.cst_id,
A.cst_key,
A.cst_firstname,
A.cst_lastname,
A.cst_material_status,
A.cst_gndr,
A.cst_create_date,
B.BDATE,
B.GEN,
C.CNTRY
from silver.crm_cust_info as A
left join silver.erp_CUST_AZ12 AS B
on A.cst_key = B.CID
left join silver.erp_LOC_A101 as C
on A.cst_key = C.CID)t
group by cst_id
having count(*) > 1


-- solving the gender problem
select distinct
A.cst_gndr,
B.GEN,
case
	when A.cst_gndr!='n/a' then A.cst_gndr  --cst_gender is the master
	else coalesce(B.GEN,'n/a' )--return the first non‑NULL value from a list of expressions.
end as integrated_gender
from silver.crm_cust_info as A
left join silver.erp_CUST_AZ12 AS B
on A.cst_key = B.CID
left join silver.erp_LOC_A101 as C
on A.cst_key = C.CID
order by 1,2


select  * from gold.dim_customers