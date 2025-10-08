import duckdb

# Create connection to persistent database
conn = duckdb.connect('./data/my_tpc.db')

# Create target table if it doesn't exist
conn.execute("""
CREATE TABLE IF NOT EXISTS monthly_sales_analysis_country (
    year INTEGER,
    month INTEGER,
    month_seq INTEGER,
    customer_sk INTEGER,
    customer_country VARCHAR,
    customer_birth_country VARCHAR,
    item_sk INTEGER,
    store_sk INTEGER,
    total_sales_amount DECIMAL,
    total_quantity INTEGER,
    total_net_paid DECIMAL,
    total_net_profit DECIMAL,
    transaction_count INTEGER
)
""")

# Extract and transform data according to gherkin specification
transform_query = """
INSERT INTO monthly_sales_analysis_country
SELECT 
    d.d_year AS year,
    d.d_moy AS month,
    d.d_month_seq AS month_seq,
    ss.ss_customer_sk AS customer_sk,
    ca.ca_country AS customer_country,
    c.c_birth_country AS customer_birth_country,
    ss.ss_item_sk AS item_sk,
    ss.ss_store_sk AS store_sk,
    SUM(ss.ss_ext_sales_price) AS total_sales_amount,
    SUM(ss.ss_quantity) AS total_quantity,
    SUM(ss.ss_net_paid) AS total_net_paid,
    SUM(ss.ss_net_profit) AS total_net_profit,
    COUNT(*) AS transaction_count
FROM store_sales ss
    INNER JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
    INNER JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
    INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE ca.ca_country IS NOT NULL 
    AND ca.ca_country != ''
GROUP BY 
    d.d_year,
    d.d_moy,
    d.d_month_seq,
    ss.ss_customer_sk,
    ca.ca_country,
    c.c_birth_country,
    ss.ss_item_sk,
    ss.ss_store_sk
ORDER BY 
    d.d_year,
    d.d_month_seq,
    SUM(ss.ss_ext_sales_price) DESC
"""

# Execute the transformation
conn.execute(transform_query)

# Commit changes and close connection
conn.commit()
conn.close()

print("ETL process completed successfully")