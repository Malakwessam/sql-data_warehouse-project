/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME
	begin try
		-- SET @start_time = GETDATE();

		PRINT 'Loading Silver Layer';
		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		--silver.crm_cust_info
		print'truncating table silver.crm_cust_info'
		truncate table silver.crm_cust_info
		print'inserting table silver.crm_cust_info'
		insert into silver.crm_cust_info(cst_id,
		cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status))= 'M' then  'Married'
			when upper(trim(cst_material_status))= 'S' then  'Single'
			else  'n/a'
		end as cst_material_status,
		case when upper(trim(cst_gndr))='M' then 'Male'
			when upper(trim(cst_gndr))='F' then 'Female'
			else 'n/a'
		end
		as cst_gndr,
		cst_create_date
		from(
		select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as ranks
		from bronze.crm_cust_info
		where cst_id is not null) t
		where t.ranks =1;

		print'truncating table silver.crm_prd_info'
		--silver.crm_prd_info
		truncate table silver.crm_prd_info
		print'inserting table silver.crm_cust_info'
		insert into silver.crm_prd_info(
			prd_id,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		select 
		prd_id,
		replace(substring(prd_key,1,5),'-','_' )as cat_id,  --derived columns
		substring(prd_key,7,LEN(prd_key) )as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))    --normalization
		when 'S' then 'other sales'
			when'M' then 'mountain'
			when 'R' then 'road'
			when 'T' then 'touring'
			else 'unknown'
		end as prd_line,
		prd_start_dt,
		dateadd(day,-1,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt  -- data enrichment
		from bronze.crm_prd_info



		-- silver.crm_sales_details
		print'truncating table silver.crm_sales_details'
		truncate table silver.crm_sales_details
		print'inserting table silver.crm_sales_details'
		insert into silver.crm_sales_details(sls_order_num,sls_prd_key,sls_cust_id ,sls_order_dt ,sls_ship_date,
			sls_due_dt,sls_sales,sls_quantity,sls_price )
		select
			sls_order_num ,
			sls_prd_key ,
			sls_cust_id ,
			case when len(sls_order_dt)!=8 or sls_order_dt=0 then null  -- datatype casting 
				else  cast(cast(sls_order_dt as varchar(50))as date)    --handling invalid dates
			end as sls_order_dt,
			case when len(sls_ship_date)!=8 or sls_ship_date=0 then null
				else  cast(cast(sls_ship_date as varchar(50))as date) 
			end as sls_ship_date,
			case when len(sls_due_dt )!=8 or sls_due_dt =0 then null
				else  cast(cast(sls_due_dt  as varchar(50))as date) 
			end as sls_due_dt ,
			case when sls_sales is null or sls_sales <=0 or sls_sales!= sls_quantity*ABS(sls_price)  --deriving and recalculating data if it invalid or incorrect
				then  sls_quantity*ABS(sls_price)
				else sls_sales
			end as sls_sales,
		sls_quantity,
		case when sls_price <0 then abs(sls_price)
			when sls_price is null then sls_sales/nullif(sls_quantity,0)
			else sls_price
		end as sls_price
			from bronze.crm_sales_details


			--silver.erp_CUST_AZ12

			PRINT '------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------'; 
			print'truncating table silver.erp_CUST_AZ12'
		truncate table silver.erp_CUST_AZ12 
		print'inserting table silver.erp_CUST_AZ12'
		insert into silver.erp_CUST_AZ12(CID,BDATE,GEN)

		 select 
		 case when CID like 'NAS%' then substring(CID,4,len(CID))
				else CID
		end as CID,
		case when BDATE >getdate() then null     --set futer birthday to null
			else BDATE
		end as BDATE,
		case when upper(GEN )in ('F','FEMALE') then 'Female'
			when upper(GEN) in ('M','MALE') then 'Male'
			else 'n/a'
		END as GEN
		 from bronze.erp_CUST_AZ12


		 --silver.erp_LOC_A101
		 print'truncating table silver.erp_LOC_A101'
		 truncate table silver.erp_LOC_A101
		 print'inserting table silver.erp_LOC_A101'
		 insert into silver.erp_LOC_A101(CID,CNTRY)
		 select 
		 replace(CID,'-','') as CID,
		 case when trim(CNTRY)='DE' THEN 'Germany'
			when trim(CNTRY) in ('US','USA') then 'United States'
			when trim(CNTRY) is null or trim(CNTRY)='' then 'n/a'
			else trim(CNTRY)
		end as CNTRY
		 from bronze.erp_LOC_A101
 

		 --silver.erp_PX_CAT_G1V2
		 print'truncating table silver.erp_PX_CAT_G1V2'
		 truncate table silver.erp_PX_CAT_G1V2
		 print'inserting table silver.erp_PX_CAT_G1V2'
		 insert into silver.erp_PX_CAT_G1V2(ID,
		 CAT,
		 SUBCAT,
		 MAINTENANCE)
		 select 
		 ID,
		 CAT,
		 SUBCAT,
		 MAINTENANCE
		 from bronze.erp_PX_CAT_G1V2
		-- SET @end_time = GETDATE();
		-- print 'load finished in' + cast( datediff(second,@start_time,@end_time) as varchar) + 'seconds';
	end try
	begin catch
		print'error occured while loading bronze level';
		print'error line:'+ cast(ERROR_LINE() AS nvarchar);
        print 'error message'+cast(ERROR_MESSAGE() AS nvarchar);
	end catch

end

go
