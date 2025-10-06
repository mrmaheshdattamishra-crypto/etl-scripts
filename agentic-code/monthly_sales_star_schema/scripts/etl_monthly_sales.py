import duckdb
import os

# Create data directory if it doesn't exist
os.makedirs('./data', exist_ok=True)

# Connect to persistent DuckDB database
conn = duckdb.connect('./data/my_tpc.db')

# Create dimension tables
def create_dim_item():
    """Create item dimension table"""
    query = """
    CREATE OR REPLACE TABLE dim_item AS
    SELECT 
        i_item_sk AS item_key,
        i_item_id AS item_id,
        i_product_name AS item_name,
        i_brand AS brand,
        i_category AS category
    FROM item
    WHERE i_item_sk IS NOT NULL
    """
    conn.execute(query)
    print("Created dim_item table")

def create_dim_store():
    """Create store dimension table"""
    query = """
    CREATE OR REPLACE TABLE dim_store AS
    SELECT 
        s_store_sk AS store_key,
        s_store_id AS store_id,
        s_store_name AS store_name,
        s_city AS city,
        s_state AS state
    FROM store
    WHERE s_store_sk IS NOT NULL
    """
    conn.execute(query)
    print("Created dim_store table")

def create_dim_date():
    """Create date dimension table"""
    query = """
    CREATE OR REPLACE TABLE dim_date AS
    SELECT 
        d_date_sk AS date_key,
        d_date AS full_date,
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
        END AS month_name
    FROM date_dim
    WHERE d_date_sk IS NOT NULL
    """
    conn.execute(query)
    print("Created dim_date table")

def create_fact_monthly_sales():
    """Create monthly sales fact table"""
    query = """
    CREATE OR REPLACE TABLE fact_monthly_sales AS
    SELECT 
        d.d_date_sk AS date_key,
        i.i_item_sk AS item_key,
        s.s_store_sk AS store_key,
        d.d_year AS sales_year,
        d.d_moy AS sales_month,
        SUM(ss.ss_quantity) AS total_quantity,
        SUM(ss.ss_ext_sales_price) AS total_sales,
        SUM(ss.ss_net_paid) AS total_net_paid,
        COUNT(*) AS transaction_count
    FROM store_sales ss
    INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk  
    INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
    WHERE d.d_date_sk IS NOT NULL
      AND i.i_item_sk IS NOT NULL
      AND s.s_store_sk IS NOT NULL
    GROUP BY 
        d.d_date_sk,
        i.i_item_sk,
        s.s_store_sk,
        d.d_year,
        d.d_moy
    """
    conn.execute(query)
    print("Created fact_monthly_sales table")

# Execute ETL process
def run_etl():
    """Run the complete ETL process"""
    print("Starting ETL process for monthly sales star schema...")
    
    # Create dimension tables
    create_dim_item()
    create_dim_store()
    create_dim_date()
    
    # Create fact table
    create_fact_monthly_sales()
    
    print("ETL process completed successfully!")

# Run the ETL
if __name__ == "__main__":
    run_etl()
    
    # Display table counts for verification
    tables = ['dim_item', 'dim_store', 'dim_date', 'fact_monthly_sales']
    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"{table}: {result[0]} rows")
    
    # Close connection
    conn.close()