import duckdb
import os

# Create database directory if it doesn't exist
os.makedirs('./data', exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect('./data/my_tpc.db')

# Create dimension tables
conn.execute("""
    CREATE OR REPLACE TABLE dim_item AS
    SELECT 
        i_item_sk AS item_key,
        i_item_id AS item_id,
        i_item_desc AS item_description,
        i_brand AS brand,
        i_class AS class,
        i_category AS category,
        i_product_name AS product_name
    FROM item
""")

conn.execute("""
    CREATE OR REPLACE TABLE dim_store AS
    SELECT 
        s_store_sk AS store_key,
        s_store_id AS store_id,
        s_store_name AS store_name,
        s_city AS city,
        s_state AS state,
        s_division_name AS division_name,
        s_company_name AS company_name
    FROM store
""")

conn.execute("""
    CREATE OR REPLACE TABLE dim_date_month AS
    SELECT DISTINCT
        CONCAT(d_year, '-', LPAD(d_moy::VARCHAR, 2, '0')) AS sales_month_key,
        d_year AS year,
        d_moy AS month,
        CASE d_moy
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February' 
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        d_quarter_name AS quarter_name
    FROM date_dim
""")

# Create fact table
conn.execute("""
    CREATE OR REPLACE TABLE fact_monthly_sales AS
    SELECT 
        CONCAT(d.d_year, '-', LPAD(d.d_moy::VARCHAR, 2, '0')) AS sales_month_key,
        ss.ss_item_sk AS item_key,
        ss.ss_store_sk AS store_key,
        d.d_year AS year,
        d.d_moy AS month,
        SUM(ss.ss_quantity) AS total_quantity,
        SUM(ss.ss_ext_sales_price) AS total_sales_amount,
        SUM(ss.ss_net_paid) AS total_net_paid,
        SUM(ss.ss_net_profit) AS total_net_profit,
        COUNT(DISTINCT ss.ss_sold_date_sk) AS transaction_count
    FROM store_sales ss
    INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
    INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk  
    INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    GROUP BY 
        CONCAT(d.d_year, '-', LPAD(d.d_moy::VARCHAR, 2, '0')),
        ss.ss_item_sk,
        ss.ss_store_sk,
        d.d_year,
        d.d_moy
""")

# Close connection
conn.close()