import duckdb

# Create persistent connection to DuckDB
conn = duckdb.connect('./data/my_tpc.db')

# Create fact table for monthly sales
conn.execute("""
    CREATE OR REPLACE TABLE fact_monthly_sales AS
    SELECT 
        d.d_date_sk AS date_key,
        ss.ss_item_sk AS item_key,
        ss.ss_store_sk AS store_key,
        d.d_year AS year,
        d.d_moy AS month,
        d.d_month_seq AS month_seq,
        SUM(ss.ss_quantity) AS total_quantity,
        SUM(ss.ss_ext_sales_price) AS total_sales_amount,
        SUM(ss.ss_net_paid) AS total_net_paid,
        SUM(ss.ss_net_profit) AS total_net_profit,
        COUNT(*) AS transaction_count
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    GROUP BY 
        d.d_date_sk,
        ss.ss_item_sk,
        ss.ss_store_sk,
        d.d_year,
        d.d_moy,
        d.d_month_seq
""")

# Create dimension table for items
conn.execute("""
    CREATE OR REPLACE TABLE dim_item AS
    SELECT 
        i_item_sk AS item_key,
        i_item_id AS item_id,
        i_item_desc AS item_desc,
        i_brand AS brand,
        i_category AS category,
        i_class AS class,
        i_product_name AS product_name
    FROM item
""")

# Create dimension table for stores
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

# Create dimension table for time periods
conn.execute("""
    CREATE OR REPLACE TABLE dim_date AS
    SELECT 
        d_date_sk AS date_key,
        d_date AS date,
        d_year AS year,
        d_moy AS month,
        d_month_seq AS month_seq
    FROM date_dim
""")

# Close connection
conn.close()