import duckdb
import os
from datetime import datetime

# Create data directory if it doesn't exist
os.makedirs('./data', exist_ok=True)

# Create persistent DuckDB connection
conn = duckdb.connect('./data/my_tpc.db')

def create_dim_item():
    """Create dimension table for items"""
    print("Creating dim_item table...")
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS dim_item")
    
    # Create dim_item table
    create_dim_item_sql = """
    CREATE TABLE dim_item AS
    SELECT 
        i_item_sk AS item_key,
        i_item_id AS item_id,
        i_product_name AS item_name,
        i_item_desc AS item_description,
        i_brand AS brand,
        i_class AS class,
        i_category AS category
    FROM item
    """
    
    conn.execute(create_dim_item_sql)
    print("dim_item table created successfully")

def create_dim_store():
    """Create dimension table for stores"""
    print("Creating dim_store table...")
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS dim_store")
    
    # Create dim_store table
    create_dim_store_sql = """
    CREATE TABLE dim_store AS
    SELECT 
        s_store_sk AS store_key,
        s_store_id AS store_id,
        s_store_name AS store_name,
        s_city AS city,
        s_state AS state,
        s_country AS country,
        s_market_desc AS market_description,
        s_division_name AS division,
        s_company_name AS company
    FROM store
    """
    
    conn.execute(create_dim_store_sql)
    print("dim_store table created successfully")

def create_dim_month():
    """Create dimension table for months"""
    print("Creating dim_month table...")
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS dim_month")
    
    # Create dim_month table
    create_dim_month_sql = """
    CREATE TABLE dim_month AS
    SELECT DISTINCT
        CONCAT(CAST(d_year AS VARCHAR), '-', LPAD(CAST(d_moy AS VARCHAR), 2, '0')) AS month_year_key,
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
        d_quarter_name AS quarter,
        d_month_seq AS month_sequence
    FROM date_dim
    ORDER BY d_year, d_moy
    """
    
    conn.execute(create_dim_month_sql)
    print("dim_month table created successfully")

def create_fact_monthly_sales():
    """Create fact table for monthly sales"""
    print("Creating fact_monthly_sales table...")
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS fact_monthly_sales")
    
    # Create fact_monthly_sales table
    create_fact_sql = """
    CREATE TABLE fact_monthly_sales AS
    SELECT 
        CONCAT(CAST(d.d_year AS VARCHAR), '-', LPAD(CAST(d.d_moy AS VARCHAR), 2, '0')) AS month_year_key,
        ss.ss_item_sk AS item_key,
        ss.ss_store_sk AS store_key,
        SUM(ss.ss_quantity) AS total_quantity,
        SUM(ss.ss_ext_sales_price) AS total_sales_amount,
        SUM(ss.ss_net_paid) AS total_net_paid,
        SUM(ss.ss_net_profit) AS total_net_profit,
        AVG(ss.ss_sales_price) AS avg_sales_price,
        COUNT(*) AS transaction_count
    FROM store_sales ss
    INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    GROUP BY 
        d.d_year,
        d.d_moy,
        ss.ss_item_sk,
        ss.ss_store_sk
    ORDER BY 
        d.d_year,
        d.d_moy,
        ss.ss_item_sk,
        ss.ss_store_sk
    """
    
    conn.execute(create_fact_sql)
    print("fact_monthly_sales table created successfully")

def validate_star_schema():
    """Validate the created star schema"""
    print("\nValidating star schema...")
    
    # Check record counts
    tables = ['dim_item', 'dim_store', 'dim_month', 'fact_monthly_sales']
    
    for table in tables:
        count_result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"{table}: {count_result[0]} records")
    
    # Test sample KPI queries
    print("\nTesting sample KPI queries...")
    
    # Sales by item per month (top 5)
    kpi1_sql = """
    SELECT 
        i.item_name,
        m.month_year_key,
        f.total_sales_amount
    FROM fact_monthly_sales f
    JOIN dim_item i ON f.item_key = i.item_key
    JOIN dim_month m ON f.month_year_key = m.month_year_key
    ORDER BY f.total_sales_amount DESC
    LIMIT 5
    """
    
    print("Top 5 sales by item per month:")
    result1 = conn.execute(kpi1_sql).fetchall()
    for row in result1:
        print(f"  {row[0][:30]} | {row[1]} | ${row[2]:,.2f}")
    
    # Sales by store per month (top 5)
    kpi2_sql = """
    SELECT 
        s.store_name,
        m.month_year_key,
        f.total_sales_amount
    FROM fact_monthly_sales f
    JOIN dim_store s ON f.store_key = s.store_key
    JOIN dim_month m ON f.month_year_key = m.month_year_key
    ORDER BY f.total_sales_amount DESC
    LIMIT 5
    """
    
    print("\nTop 5 sales by store per month:")
    result2 = conn.execute(kpi2_sql).fetchall()
    for row in result2:
        print(f"  {row[0][:30]} | {row[1]} | ${row[2]:,.2f}")

def main():
    """Main ETL execution function"""
    print("Starting Monthly Sales Star Schema ETL Process")
    print("=" * 50)
    
    try:
        # Create dimension tables
        create_dim_item()
        create_dim_store()
        create_dim_month()
        
        # Create fact table
        create_fact_monthly_sales()
        
        # Validate the schema
        validate_star_schema()
        
        print("\n" + "=" * 50)
        print("Monthly Sales Star Schema ETL Process Completed Successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise
    
    finally:
        # Close connection
        conn.close()

if __name__ == "__main__":
    main()