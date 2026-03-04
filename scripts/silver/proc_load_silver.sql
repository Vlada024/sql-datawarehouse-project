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
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CALL silver.load_silver();
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time        TIMESTAMP;
    v_end_time          TIMESTAMP;
    v_batch_start_time  TIMESTAMP;
    v_batch_end_time    TIMESTAMP;
BEGIN
    v_batch_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

v_start_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    )
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END cst_gndr,
    cst_create_date
FROM(
SELECT
    *,
    row_number() over (PARTITION BY cst_id ORDER BY cst_create_date desc) ranked
FROM bronze.crm_cust_info) t
WHERE ranked = 1;
v_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
RAISE NOTICE '>> -------------';

 -- Loading silver.crm_prd_info
v_start_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
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
    REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') as cat_id,
    substr(prd_key, 7, LENGTH(prd_key)) as prd_key,
    prd_nm,
    coalesce(prd_cost, 0) AS prc_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'N/A'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) prd_srart_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) as prd_end_dt
FROM bronze.crm_prd_info;
v_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Load Duration : % seconds', EXTRACT(EPOCH FROM(v_end_time - v_start_time))::INT;
RAISE NOTICE '>> -------------';
-- Check unwanted spaces

v_start_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
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
    CASE WHEN sls_order_dt = 0 OR length(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR length(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR length(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * abs(sls_price)
            THEN sls_quantity * abs(sls_price)
    ELSE sls_sales
    END as sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;
v_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
RAISE NOTICE '>> -------------';


v_start_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;
v_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
RAISE NOTICE '>> -------------';

RAISE NOTICE '------------------------------------------------';
RAISE NOTICE 'Loading ERP Tables';
RAISE NOTICE '------------------------------------------------';


v_start_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
    replace(cid, '-', '') cid,
    CASE WHEN TRIM(cntry) IN('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
    ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;
v_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
RAISE NOTICE '>> -------------';


v_start_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
v_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
RAISE NOTICE '>> -------------';

v_batch_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE '==========================================';
RAISE NOTICE 'Loading Silver Layer is Completed';
RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time))::INT;
RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
RAISE NOTICE '==========================================';
RAISE NOTICE 'ERROR OCCURED DURING LOADING SILVER LAYER';
RAISE NOTICE 'Error Message: %', SQLERRM;
RAISE NOTICE 'Error Code: %', SQLSTATE;
RAISE NOTICE '==========================================';
END;
$$;
