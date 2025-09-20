import duckdb

# Create connection to persistent DuckDB database
conn = duckdb.connect('./data/my_tpc.db')

# Create unified sales fact table
conn.execute("""
CREATE OR REPLACE TABLE unified_sales_fact AS
WITH sales_union AS (
    -- Store sales
    SELECT 
        'STORE-' || ss_item_sk::STRING || '-' || ss_sold_date_sk::STRING || '-' || ss_customer_sk::STRING AS sale_id,
        d.d_date AS sale_date,
        'STORE' AS channel,
        ss_customer_sk AS customer_key,
        ss_item_sk AS item_key,
        ss_store_sk AS store_key,
        ss_quantity AS quantity,
        ss_sales_price AS unit_price,
        ss_ext_sales_price AS total_sales,
        ss_net_profit AS profit
    FROM store_sales ss
    LEFT JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    
    UNION ALL
    
    -- Catalog sales
    SELECT 
        'CATALOG-' || cs_item_sk::STRING || '-' || cs_sold_date_sk::STRING || '-' || cs_bill_customer_sk::STRING AS sale_id,
        d.d_date AS sale_date,
        'CATALOG' AS channel,
        cs_bill_customer_sk AS customer_key,
        cs_item_sk AS item_key,
        NULL AS store_key,
        cs_quantity AS quantity,
        cs_sales_price AS unit_price,
        cs_ext_sales_price AS total_sales,
        cs_net_profit AS profit
    FROM catalog_sales cs
    LEFT JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    
    UNION ALL
    
    -- Web sales
    SELECT 
        'WEB-' || ws_item_sk::STRING || '-' || ws_sold_date_sk::STRING || '-' || ws_bill_customer_sk::STRING AS sale_id,
        d.d_date AS sale_date,
        'WEB' AS channel,
        ws_bill_customer_sk AS customer_key,
        ws_item_sk AS item_key,
        NULL AS store_key,
        ws_quantity AS quantity,
        ws_sales_price AS unit_price,
        ws_ext_sales_price AS total_sales,
        ws_net_profit AS profit
    FROM web_sales ws
    LEFT JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
)
SELECT * FROM sales_union
WHERE sale_date IS NOT NULL
""")

# Create customer dimension
conn.execute("""
CREATE OR REPLACE TABLE customer_dimension AS
SELECT 
    c.c_customer_sk AS customer_key,
    c.c_customer_id AS customer_id,
    COALESCE(c.c_first_name || ' ' || c.c_last_name, 'Unknown') AS full_name,
    c.c_birth_year AS birth_year,
    COALESCE(ca.ca_city, 'Unknown') AS address_city,
    COALESCE(ca.ca_state, 'Unknown') AS address_state
FROM customer c
LEFT JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
""")

# Create item dimension
conn.execute("""
CREATE OR REPLACE TABLE item_dimension AS
SELECT 
    i_item_sk AS item_key,
    i_item_id AS item_id,
    COALESCE(i_item_desc, 'Unknown') AS item_description,
    COALESCE(i_category, 'Unknown') AS category,
    COALESCE(i_brand, 'Unknown') AS brand
FROM item
""")

# Create store dimension
conn.execute("""
CREATE OR REPLACE TABLE store_dimension AS
SELECT 
    s_store_sk AS store_key,
    s_store_id AS store_id,
    s_store_name AS store_name,
    s_city AS city,
    s_state AS state,
    s_market_desc AS market_description
FROM store
""")

# Create date dimension
conn.execute("""
CREATE OR REPLACE TABLE date_dimension AS
SELECT 
    d_date_sk AS date_key,
    d_date AS calendar_date,
    d_year AS year,
    CAST((d_month_seq - 1) % 12 + 1 AS INTEGER) AS month,
    CAST((d_quarter_seq - 1) % 4 + 1 AS INTEGER) AS quarter
FROM date_dim
WHERE d_date IS NOT NULL
""")

# Create sales summary by channel
conn.execute("""
CREATE OR REPLACE TABLE sales_summary_by_channel AS
SELECT 
    channel,
    sale_date,
    SUM(quantity) AS total_quantity,
    SUM(total_sales) AS total_sales,
    SUM(profit) AS total_profit
FROM unified_sales_fact
WHERE sale_date IS NOT NULL
GROUP BY channel, sale_date
ORDER BY sale_date, channel
""")

# Close connection
conn.close()