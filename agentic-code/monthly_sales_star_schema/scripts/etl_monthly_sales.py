import duckdb

# Create persistent DuckDB connection
conn = duckdb.connect('./data/my_tpc.db')

try:
    # Create dimension table for items
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

    # Create dimension table for stores
    conn.execute("""
        CREATE OR REPLACE TABLE dim_store AS
        SELECT 
            s_store_sk AS store_key,
            s_store_id AS store_id,
            s_store_name AS store_name,
            s_city AS city,
            s_state AS state,
            s_market_desc AS market_description
        FROM store
    """)

    # Create dimension table for monthly dates
    conn.execute("""
        CREATE OR REPLACE TABLE dim_date_month AS
        SELECT 
            d_date_sk AS date_key,
            CONCAT(CAST(d_year AS VARCHAR), '-', LPAD(CAST(d_moy AS VARCHAR), 2, '0')) AS year_month,
            d_year AS year,
            d_moy AS month,
            d_month_seq AS month_sequence
        FROM date_dim
    """)

    # Create fact table for monthly sales
    conn.execute("""
        CREATE OR REPLACE TABLE fact_monthly_sales AS
        SELECT 
            dd.d_date_sk AS date_key,
            ss.ss_item_sk AS item_key,
            ss.ss_store_sk AS store_key,
            CONCAT(CAST(dd.d_year AS VARCHAR), '-', LPAD(CAST(dd.d_moy AS VARCHAR), 2, '0')) AS year_month,
            SUM(ss.ss_ext_sales_price) AS total_sales_amount,
            SUM(ss.ss_quantity) AS total_quantity,
            SUM(ss.ss_net_paid) AS total_net_paid
        FROM store_sales ss
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        GROUP BY 
            dd.d_date_sk,
            ss.ss_item_sk,
            ss.ss_store_sk,
            dd.d_year,
            dd.d_moy
    """)

    print("ETL process completed successfully!")
    print("Created tables:")
    print("- dim_item")
    print("- dim_store") 
    print("- dim_date_month")
    print("- fact_monthly_sales")

finally:
    conn.close()