import duckdb

# Create DuckDB connection with persistent file
conn = duckdb.connect('/Users/maheshdattamishra/my_tpc.db')

# Create the sales_summary_by_channel table with appropriate schema
conn.execute("""
CREATE TABLE IF NOT EXISTS sales_summary_by_channel (
    sale_date DATE,
    year INTEGER,
    month INTEGER,
    quarter INTEGER,
    day_name VARCHAR,
    channel VARCHAR,
    item_category VARCHAR,
    item_brand VARCHAR,
    item_class VARCHAR,
    total_quantity INTEGER,
    total_sales_amount DECIMAL(15,2),
    total_net_profit DECIMAL(15,2),
    avg_unit_price DECIMAL(15,2),
    transaction_count INTEGER
)
""")

# Clear existing data
conn.execute("DELETE FROM sales_summary_by_channel")

# Insert unified sales data from all channels
conn.execute("""
INSERT INTO sales_summary_by_channel
WITH unified_sales AS (
    -- Store sales data
    SELECT 
        d.d_date as sale_date,
        d.d_year as year,
        d.d_moy as month,
        d.d_qoy as quarter,
        d.d_day_name as day_name,
        'store' as channel,
        i.i_category as item_category,
        i.i_brand as item_brand,
        i.i_class as item_class,
        ss.ss_quantity as quantity,
        ss.ss_ext_sales_price as ext_sales_price,
        ss.ss_net_profit as net_profit,
        ss.ss_sales_price as unit_price,
        1 as transaction_indicator
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN item i ON ss.ss_item_sk = i.i_item_sk
    
    UNION ALL
    
    -- Catalog sales data
    SELECT 
        d.d_date as sale_date,
        d.d_year as year,
        d.d_moy as month,
        d.d_qoy as quarter,
        d.d_day_name as day_name,
        'catalog' as channel,
        i.i_category as item_category,
        i.i_brand as item_brand,
        i.i_class as item_class,
        cs.cs_quantity as quantity,
        cs.cs_ext_sales_price as ext_sales_price,
        cs.cs_net_profit as net_profit,
        cs.cs_sales_price as unit_price,
        1 as transaction_indicator
    FROM catalog_sales cs
    JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN item i ON cs.cs_item_sk = i.i_item_sk
    
    UNION ALL
    
    -- Web sales data
    SELECT 
        d.d_date as sale_date,
        d.d_year as year,
        d.d_moy as month,
        d.d_qoy as quarter,
        d.d_day_name as day_name,
        'web' as channel,
        i.i_category as item_category,
        i.i_brand as item_brand,
        i.i_class as item_class,
        ws.ws_quantity as quantity,
        ws.ws_ext_sales_price as ext_sales_price,
        ws.ws_net_profit as net_profit,
        ws.ws_sales_price as unit_price,
        1 as transaction_indicator
    FROM web_sales ws
    JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN item i ON ws.ws_item_sk = i.i_item_sk
)
SELECT 
    sale_date,
    year,
    month,
    quarter,
    day_name,
    channel,
    item_category,
    item_brand,
    item_class,
    CAST(SUM(quantity) AS INTEGER) as total_quantity,
    CAST(SUM(ext_sales_price) AS DECIMAL(15,2)) as total_sales_amount,
    CAST(SUM(net_profit) AS DECIMAL(15,2)) as total_net_profit,
    CAST(CASE 
        WHEN SUM(quantity) > 0 
        THEN SUM(ext_sales_price) / SUM(quantity) 
        ELSE 0 
    END AS DECIMAL(15,2)) as avg_unit_price,
    CAST(SUM(transaction_indicator) AS INTEGER) as transaction_count
FROM unified_sales
GROUP BY 
    sale_date, 
    year,
    month,
    quarter,
    day_name,
    channel, 
    item_category, 
    item_brand, 
    item_class
""")

# Close connection
conn.close()

print("ETL process completed successfully. Sales summary data has been loaded into sales_summary_by_channel table.")