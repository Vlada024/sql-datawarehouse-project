/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time        TIMESTAMP;
    v_end_time          TIMESTAMP;
    v_batch_start_time  TIMESTAMP;
    v_batch_end_time    TIMESTAMP;
BEGIN
    v_batch_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '=================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=================================';

    RAISE NOTICE '---------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------';

    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Getting the data into bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/tmp/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER TRUE);
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';


    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;


    RAISE NOTICE '>> Getting the data into bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/tmp/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER TRUE);
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';


    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table bronze.crm_sales_info';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Getting the data into bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/tmp/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER TRUE);
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';


    RAISE NOTICE '---------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---------------------------------';

    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Getting the data into bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/tmp/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER TRUE);
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';


    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Getting the data into bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/tmp/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT CSV, HEADER TRUE);
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';


    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Getting the data into bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/tmp/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER TRUE);
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';


    v_batch_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time))::INT;
    RAISE NOTICE '==========================================';


    EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error Code: %', SQLSTATE;
    RAISE NOTICE '==========================================';
END;
$$;
