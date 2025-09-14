import duckdb

# Create DuckDB connection with persistent file
conn = duckdb.connect('/Users/maheshdattamishra/my_tpc.db')

try:
    # Create dim_item dimension table
    conn.execute("""
        CREATE OR REPLACE TABLE dim_item AS
        SELECT 
            i_item_sk AS item_key,
            i_item_id AS item_id,
            i_product_name AS item_name,
            i_item_desc AS item_description,
            i_brand AS brand,
            i_category AS category,
            i_class AS class
        FROM item
    """)

    # Create dim_store dimension table
    conn.execute("""
        CREATE OR REPLACE TABLE dim_store AS
        SELECT 
            s_store_sk AS store_key,
            s_store_id AS store_id,
            s_store_name AS store_name,
            s_city AS city,
            s_state AS state,
            s_country AS country
        FROM store
    """)

    # Create dim_date dimension table
    conn.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT 
            d_date_sk AS date_key,
            d_date AS date_value,
            d_year AS year,
            d_moy AS month,
            CONCAT(CAST(d_year AS VARCHAR), '-', LPAD(CAST(d_moy AS VARCHAR), 2, '0')) AS year_month,
            d_quarter_name AS quarter_name
        FROM date_dim
    """)

    # Create fact_monthly_sales fact table
    conn.execute("""
        CREATE OR REPLACE TABLE fact_monthly_sales AS
        SELECT 
            d.d_date_sk AS date_key,
            ss.ss_item_sk AS item_key,
            ss.ss_store_sk AS store_key,
            CONCAT(CAST(d.d_year AS VARCHAR), '-', LPAD(CAST(d.d_moy AS VARCHAR), 2, '0')) AS year_month,
            SUM(ss.ss_quantity) AS total_quantity,
            SUM(ss.ss_ext_sales_price) AS total_sales_amount,
            SUM(ss.ss_net_paid) AS total_net_paid,
            SUM(ss.ss_net_profit) AS total_net_profit
        FROM store_sales ss
        JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        GROUP BY 
            d.d_date_sk,
            ss.ss_item_sk,
            ss.ss_store_sk,
            CONCAT(CAST(d.d_year AS VARCHAR), '-', LPAD(CAST(d.d_moy AS VARCHAR), 2, '0'))
    """)

    print("ETL process completed successfully!")
    print("Created tables:")
    print("- dim_item: Item dimension table")
    print("- dim_store: Store dimension table") 
    print("- dim_date: Date dimension table")
    print("- fact_monthly_sales: Monthly sales fact table")

    # Verify table creation and show record counts
    tables = ['dim_item', 'dim_store', 'dim_date', 'fact_monthly_sales']
    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"{table}: {result[0]} records")

finally:
    conn.close()