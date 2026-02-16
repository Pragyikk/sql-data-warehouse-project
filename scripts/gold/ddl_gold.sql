/*
	==================================================
	DDL Script: Create gold views
	==================================================
	Script Purpose:
		This script creates views for gold layer in the data warehouse
		This layer represents a facts table and two dimension tables in 'star schema'

		Each view performs transformation on and combine silver layer tables to produce
		a business-ready dataset

	Usage: These views can be queried directly for analytics and reporting
*/

-- ===================================================
-- Create dimension table gold.dim_customers
-- ===================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS firstname,
		ci.cst_lastname AS lastname, 
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE
			WHEN ci.cst_gndr IN ('MALE', 'FEMALE') OR ca.gen IS NULL THEN ci.cst_gndr --crm is master for gender info
			ELSE ca.gen
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key= ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key= la.cid
GO

-- ===================================================
-- Create dimension table gold.dim_products
-- ===================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	px.cat AS category,
	px.subcat AS subcategory,
	px.maintenance,
	pr.prd_cost AS product_cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pr.cat_id= px.id
WHERE prd_end_dt IS NULL -- filter out all historical data
GO

-- ===================================================
-- Create facts table gold.fact_sales
-- ===================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
SELECT 
	ss.sls_ord_num AS order_number,
	dp.product_key,
	ds.customer_key,
	ss.sls_order_dt AS order_date,
	ss.sls_ship_dt AS shipping_date,
	ss.sls_due_dt AS due_date,
	ss.sls_sales AS sales_amount,
	ss.sls_quantity AS quantity,
	ss.sls_price AS price
FROM silver.crm_sales_details ss
LEFT JOIN gold.dim_products dp
ON ss.sls_prd_key= dp.product_number
LEFT JOIN gold.dim_customers ds
ON ss.sls_cust_id= ds.customer_id
GO
