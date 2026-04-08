/*
DDL Script: Create Bronze Tables
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables  (exec bronze.loadbronze)
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_order_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_date int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
);


DROP TABLE IF EXISTS bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
);


DROP TABLE IF EXISTS bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101(
	CID nvarchar(50),
	CNTRY nvarchar(50)
);


DROP TABLE IF EXISTS bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2(
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50)
);


/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
 Stored procedure used to encapsulate bronze reload:
 * single entry point (EXEC bronze.load_bronze)
 * consistent options across tables

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
*/
go

create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime ,@end_time datetime;
	set @start_time=GETDATE();
	begin try  -- runs the try if it fails it run the catch
		print 'starting loading the bronze layer';
		print'truncating:crm_cust_info ';
		print'inserting data:crm_cust_info ';
		truncate table bronze.crm_cust_info; --TRUNCATE each table to ensure a clean reload
		bulk insert bronze.crm_cust_info
		from 'C:\Users\PC\Documents\Data Analysis\SQL\projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with( firstrow=2,   --skip header
		fieldterminator=',',
		tablock); --Table-level lock for faster bulk load and potential minimal logging


		print'truncating:crm_prd_info ';
		print'inserting data:crm_prd_info ';
		truncate table  bronze.crm_prd_info;
		bulk insert  bronze.crm_prd_info
		from 'C:\Users\PC\Documents\Data Analysis\SQL\projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with( firstrow=2,fieldterminator=',',tablock);


		print'truncating:crm_sales_details ';
		print'inserting data:crm_sales_details ';
		truncate table  bronze.crm_sales_details;
		bulk insert  bronze.crm_sales_details
		from 'C:\Users\PC\Documents\Data Analysis\SQL\projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with( firstrow=2,fieldterminator=',',tablock);



		print'truncating:erp_CUST_AZ12 ';
		print'inserting data:erp_CUST_AZ12 ';
		truncate table  bronze.erp_CUST_AZ12;
		bulk insert  bronze.erp_CUST_AZ12
		from 'C:\Users\PC\Documents\Data Analysis\SQL\projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with( firstrow=2,fieldterminator=',',tablock);



		print'truncating:erp_LOC_A101 ';
		print'inserting data:erp_LOC_A101 ';
		truncate table  bronze.erp_LOC_A101;
		bulk insert  bronze.erp_LOC_A101
		from 'C:\Users\PC\Documents\Data Analysis\SQL\projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with( firstrow=2,fieldterminator=',',tablock);


		print'truncating:erp_PX_CAT_G1V2 ';
		print'inserting data:erp_PX_CAT_G1V2';
		truncate table  bronze.erp_PX_CAT_G1V2;
		bulk insert  bronze.erp_PX_CAT_G1V2
		from 'C:\Users\PC\Documents\Data Analysis\SQL\projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with( firstrow=2,fieldterminator=',',tablock);

		print 'The bronze layer successfully loaded';
	end try
	begin catch
		print'error occured while loading bronze level';
		print'error line:'+ cast(ERROR_LINE() AS nvarchar);
        print 'error message'+cast(ERROR_MESSAGE() AS nvarchar);

	end catch
set @end_time=GETDATE(); 
print 'load duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar(20)) +'seconds';
end;
go

exec bronze.load_bronze;
