/*
	Stored Procedure to load silver layer: (bronze-> silver)

	This scripts performs ETL (Extract Transform Load) to populate silver
	schema tables from bronze schema

	Actions performed:
		-Trucatate silver tables
		-Insert transformed and clean data from bronze to silver tables

	Parameters:
		-None
		-This stored procedure takes no parameters or returns any values.
		
	Usage Example:
		EXEC silver.load_silver;
*/

CREATE OR ALTER PROC silver.load_silver 
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time= GETDATE()
		PRINT '==================================='
		PRINT 'Loading Silver Layer'
		PRINT '==================================='
		PRINT 'Loading crm Tables'
		PRINT '==================================='

		-- Loading silver.crm_cust_info
		SET @start_time= GETDATE()
		PRINT '>>TRUNCATING TABLE silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> INSERTING DATA INTO: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_marital_status,
			cst_firstname,
			cst_lastname,
			cst_gndr,
			cst_create_date
		)
		-- Transformation for duplicates, null values in the primary key remove unwanted spaces
		SELECT
			cst_id,
			cst_key,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM 
		(
		SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS duplicates_rank
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE duplicates_rank= 1
		SET @end_time= GETDATE()
		PRINT '>>Load Duration= '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='

		--Loading silver.crm_prd_info
		SET @start_time= GETDATE()
		PRINT '>>TRUNCATING TABLE silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		--2
		PRINT '>>INSERING DATA INTO: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT
			prd_id,
			REPLACE(LEFT(prd_key,5),'-','_') AS cat_id,--extract category id
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,--extract product id
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE
				WHEN UPPER(TRIM(prd_line))= 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line))= 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line))= 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line))= 'T' THEN 'Touring'
				ELSE 'n/a'--map product line codes to descriptive values
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt--calculate end date as one day before the next start date
		FROM bronze.crm_prd_info
		SET @end_time= GETDATE()
		PRINT '>>Load Duration= '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='

		--Loading silver.crm_sales_details
		SET @start_time= GETDATE()
		PRINT '>>TRUNCATING TABLE silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		--3 
		PRINT '>>INSERING DATA INTO: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt= 0 OR LEN(sls_order_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,--converting int to date type after ensuring correct date type format
			CASE 
				WHEN sls_ship_dt= 0 OR LEN(sls_ship_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt= 0 OR LEN(sls_due_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales<=0 OR sls_sales IS NULL OR sls_sales!= sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,--recalculating sales if original value is missing or incorrect
			sls_quantity,
			CASE
				WHEN sls_price=0 OR sls_price IS NULL
					THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE ABS(sls_price)
			END AS sls_price--recalculating price if original value is missing or incorrect
		FROM bronze.crm_sales_details
		SET @end_time= GETDATE()
		PRINT '>>Load Duration= '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='

		PRINT 'Loading erp Tables'
		PRINT '==================================='

		--Loading silver.erp_cust_az12
		SET @start_time= GETDATE()
		PRINT '>>TRUNCATING TABLE silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		--4 
		PRINT '>>INSERTING DATA INTO: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' --Remove NAS prefix if present
					THEN SUBSTRING(cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate> GETDATE() THEN NULL--Set future birthdays to null
				ELSE bdate
			END AS bdate,
			CASE
				WHEN UPPER(TRIM(gen))='M' THEN 'Male'--Normalize gender values and handle unknown cases
				WHEN UPPER(TRIM(gen))='F' THEN 'Female'
				WHEN UPPER(TRIM(gen)) IS NULL OR UPPER(TRIM(gen))= '' THEN 'n/a'
				ELSE gen
			END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time= GETDATE()
		PRINT '>>Load Duration= '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='

		--Loading silver.erp_loc_a101
		SET @start_time= GETDATE()
		PRINT '>>TRUNCATING TABLE silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		--5 TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>>INSERTING DATA INTO: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT
			CONCAT(LEFT(cid,2), SUBSTRING(cid,4,LEN(cid))) AS cid,
			CASE
				WHEN TRIM(cntry) IS NULL OR TRIM(cntry)= '' THEN 'n/a'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) ='DE' THEN 'Germany'
				ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time= GETDATE()
		PRINT '>>Load Duration= '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='

		--Loading silver.erp_px_cat_g1v2
		SET @start_time= GETDATE()
		PRINT '>>TRUNCATING TABLE silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
 
		PRINT '>>INSERTING DATA INTO: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)

		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time= GETDATE()
		PRINT '>>Load Duration= '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='

		SET @batch_end_time= GETDATE()
		PRINT '>>Loading Silver Layer is Completed'
		PRINT '>>Total Batch Load Duration= '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)+ ' seconds'
		PRINT '==================================='
	END TRY

	BEGIN CATCH
		PRINT '==================================='
		PRINT 'Error occured during loading silver layer'
		PRINT 'ERROR_MESSAGE: '+ ERROR_MESSAGE()
		PRINT 'ERROR_NUMBER: '+ CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'ERROR_STATE: '+ CAST(ERROR_STATE() AS NVARCHAR)
		PRINT '==================================='
	END CATCH

END
