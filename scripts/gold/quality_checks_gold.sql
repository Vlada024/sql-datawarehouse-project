--Check for duplicates
SELECT cst_id, COUNT(*) FROM(
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
        ) t
GROUP BY cst_id HAVING COUNT(*) > 1;

-- DATA INTEGRATION, THERE WERE 2 GENDER COLUMNS INCOMPLETE AND WE COMBINE THEM INTO 1, IF THERE IS MISSING INFO
-- FROM THE FIRST WE USE THE 2ND AND VICE VERSA
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr --CRM IS THE MASTER FOR GENDER INFO
    ELSE COALESCE(ca.gen, 'N/A')
    END as new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;


SELECT
    distinct gender
FROM gold.dim_customers;


-- REST OF THE TABLES JOINED
SELECT prd_key, COUNT(*) FROM(
    SELECT
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL) t -- FILTER OUT ALL HISTORICAL DATA
    GROUP BY prd_key HAVING COUNT(*) > 1; -- Checking duplicates


SELECT
    *
FROM gold.dim_products;



SELECT
    *
FROM gold.fact_sales;


select
    *
from gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p ON
p.product_key = f.product_key
WHERE p.product_key IS NULL;
-- WHERE c.customer_key IS NULL;
