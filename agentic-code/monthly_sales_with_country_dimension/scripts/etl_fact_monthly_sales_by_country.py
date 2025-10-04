import duckdb
import os

# Create connection to persistent database
conn = duckdb.connect('./data/my_tpc.db')

# Create target table for monthly sales with country dimension
conn.execute("""
    CREATE OR REPLACE TABLE fact_monthly_sales_by_country (
        sale_year INTEGER,
        sale_month INTEGER,
        month_seq INTEGER,
        customer_country VARCHAR,
        item_sk INTEGER,
        store_sk INTEGER,
        total_quantity INTEGER,
        total_sales_amount DECIMAL(15,2),
        total_net_paid DECIMAL(15,2),
        total_net_profit DECIMAL(15,2),
        transaction_count INTEGER
    )
""")

# Build ETL query based on gherkin specification
etl_query = """
WITH sales_unified AS (
    -- Store Sales
    SELECT 
        ss_sold_date_sk as sold_date_sk,
        ss_customer_sk as customer_sk,
        ss_addr_sk as addr_sk,
        ss_item_sk as item_sk,
        ss_store_sk as store_sk,
        ss_quantity as quantity,
        ss_ext_sales_price as ext_sales_price,
        ss_net_paid as net_paid,
        ss_net_profit as net_profit,
        'STORE' as channel
    FROM store_sales
    
    UNION ALL
    
    -- Catalog Sales
    SELECT 
        cs_sold_date_sk as sold_date_sk,
        cs_bill_customer_sk as customer_sk,
        cs_bill_addr_sk as addr_sk,
        cs_item_sk as item_sk,
        NULL as store_sk,
        cs_quantity as quantity,
        cs_ext_sales_price as ext_sales_price,
        cs_net_paid as net_paid,
        cs_net_profit as net_profit,
        'CATALOG' as channel
    FROM catalog_sales
    
    UNION ALL
    
    -- Web Sales
    SELECT 
        ws_sold_date_sk as sold_date_sk,
        ws_bill_customer_sk as customer_sk,
        ws_bill_addr_sk as addr_sk,
        ws_item_sk as item_sk,
        NULL as store_sk,
        ws_quantity as quantity,
        ws_ext_sales_price as ext_sales_price,
        ws_net_paid as net_paid,
        ws_net_profit as net_profit,
        'WEB' as channel
    FROM web_sales
),

sales_with_dimensions AS (
    SELECT 
        s.sold_date_sk,
        s.customer_sk,
        s.addr_sk,
        s.item_sk,
        s.store_sk,
        s.quantity,
        s.ext_sales_price,
        s.net_paid,
        s.net_profit,
        s.channel,
        -- Date dimensions
        d.d_year as sale_year,
        d.d_moy as sale_month,
        d.d_month_seq as month_seq,
        -- Customer country dimension with null handling
        COALESCE(ca.ca_country, 'UNKNOWN') as customer_country
    FROM sales_unified s
    INNER JOIN date_dim d ON s.sold_date_sk = d.d_date_sk
    INNER JOIN customer c ON s.customer_sk = c.c_customer_sk
    INNER JOIN customer_address ca ON s.addr_sk = ca.ca_address_sk
    -- Exclude records with null date or customer
    WHERE s.sold_date_sk IS NOT NULL 
        AND s.customer_sk IS NOT NULL
)

INSERT INTO fact_monthly_sales_by_country (
    sale_year,
    sale_month,
    month_seq,
    customer_country,
    item_sk,
    store_sk,
    total_quantity,
    total_sales_amount,
    total_net_paid,
    total_net_profit,
    transaction_count
)
SELECT 
    sale_year,
    sale_month,
    month_seq,
    customer_country,
    item_sk,
    store_sk,
    SUM(quantity) as total_quantity,
    SUM(ext_sales_price) as total_sales_amount,
    SUM(net_paid) as total_net_paid,
    SUM(net_profit) as total_net_profit,
    COUNT(*) as transaction_count
FROM sales_with_dimensions
GROUP BY 
    sale_year,
    sale_month,
    month_seq,
    customer_country,
    item_sk,
    store_sk
"""

# Execute the ETL transformation
conn.execute(etl_query)

# Verify the results
result = conn.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_country) as unique_countries,
        COUNT(DISTINCT item_sk) as unique_items,
        MIN(sale_year) as min_year,
        MAX(sale_year) as max_year
    FROM fact_monthly_sales_by_country
""").fetchall()

print(f"ETL completed successfully. Records processed: {result[0][0]}")
print(f"Unique countries: {result[0][1]}")
print(f"Unique items: {result[0][2]}")
print(f"Date range: {result[0][3]} - {result[0][4]}")

# Close connection
conn.close()