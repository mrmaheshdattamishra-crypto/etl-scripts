import duckdb
import os

# Create data directory if it doesn't exist
os.makedirs('./data', exist_ok=True)

# Create connection to persistent DuckDB database
conn = duckdb.connect('./data/my_tpc.db')

# Create dimension tables
print("Creating dimension tables...")

# Create dim_item table
conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_item AS
    SELECT 
        i_item_sk AS item_key,
        i_item_id AS item_id,
        i_item_desc AS item_desc,
        i_brand AS brand,
        i_brand_id AS brand_id,
        i_class AS class,
        i_class_id AS class_id,
        i_category AS category,
        i_category_id AS category_id,
        i_manufact AS manufact,
        i_manufact_id AS manufact_id,
        i_product_name AS product_name,
        i_current_price AS current_price
    FROM item
""")

# Create dim_store table
conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_store AS
    SELECT 
        s_store_sk AS store_key,
        s_store_id AS store_id,
        s_store_name AS store_name,
        s_number_employees AS number_employees,
        s_floor_space AS floor_space,
        s_manager AS manager,
        s_market_id AS market_id,
        s_market_desc AS market_desc,
        s_geography_class AS geography_class,
        s_division_name AS division_name,
        s_company_name AS company_name,
        s_city AS city,
        s_state AS state,
        s_zip AS zip,
        s_country AS country
    FROM store
""")

# Create dim_date table
conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_date AS
    SELECT 
        d_date_sk AS date_key,
        d_date AS date,
        d_year AS year,
        d_moy AS month,
        d_month_seq AS month_seq,
        d_qoy AS quarter,
        d_dom AS day_of_month,
        d_dow AS day_of_week,
        d_day_name AS day_name,
        d_quarter_name AS quarter_name,
        d_weekend AS is_weekend,
        d_holiday AS is_holiday
    FROM date_dim
""")

print("Creating fact table...")

# Create fact_monthly_sales table with aggregated data
conn.execute("""
    CREATE TABLE IF NOT EXISTS fact_monthly_sales AS
    SELECT 
        d.d_date_sk AS date_key,
        ss.ss_item_sk AS item_key,
        ss.ss_store_sk AS store_key,
        d.d_year AS year,
        d.d_moy AS month,
        d.d_month_seq AS month_seq,
        SUM(ss.ss_ext_sales_price) AS total_sales_amount,
        SUM(ss.ss_quantity) AS total_quantity,
        SUM(ss.ss_net_paid) AS total_net_paid,
        SUM(ss.ss_net_profit) AS total_net_profit,
        SUM(ss.ss_ext_discount_amt) AS total_discount_amount,
        SUM(ss.ss_ext_tax) AS total_tax_amount,
        COUNT(DISTINCT ss.ss_ticket_number) AS transaction_count
    FROM store_sales ss
    INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
    INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
    GROUP BY 
        d.d_date_sk,
        d.d_year,
        d.d_moy,
        d.d_month_seq,
        ss.ss_item_sk,
        ss.ss_store_sk
""")

print("ETL process completed successfully!")

# Display some sample data from each table
print("\nSample data from dim_item:")
result = conn.execute("SELECT * FROM dim_item LIMIT 5").fetchall()
for row in result:
    print(row)

print("\nSample data from dim_store:")
result = conn.execute("SELECT * FROM dim_store LIMIT 5").fetchall()
for row in result:
    print(row)

print("\nSample data from dim_date:")
result = conn.execute("SELECT * FROM dim_date LIMIT 5").fetchall()
for row in result:
    print(row)

print("\nSample data from fact_monthly_sales:")
result = conn.execute("SELECT * FROM fact_monthly_sales LIMIT 5").fetchall()
for row in result:
    print(row)

# Close connection
conn.close()

print("\nMonthly sales star schema ETL completed successfully!")