import duckdb

# Connect to persistent DuckDB database
conn = duckdb.connect('./data/my_tpc.db')

# Create the customer_with_region table with US region categorization
conn.execute("""
CREATE OR REPLACE TABLE customer_with_region AS
SELECT 
    c.c_customer_sk,
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    ca.ca_state AS state,
    ca.ca_city AS city,
    ca.ca_county AS county,
    CASE 
        WHEN ca.ca_state IN ('AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV', 'CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT') THEN 'Northeast'
        WHEN ca.ca_state IN ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI') THEN 'Midwest'
        WHEN ca.ca_state IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY') THEN 'Mountain'
        WHEN ca.ca_state IN ('AK', 'CA', 'HI', 'OR', 'WA') THEN 'Pacific'
        WHEN ca.ca_state IN ('TX', 'OK') THEN 'South'
        ELSE 'Unknown'
    END AS us_region,
    CASE 
        WHEN ca.ca_state IN ('AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV', 'CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT') THEN 'NE'
        WHEN ca.ca_state IN ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI') THEN 'MW'
        WHEN ca.ca_state IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY') THEN 'MT'
        WHEN ca.ca_state IN ('AK', 'CA', 'HI', 'OR', 'WA') THEN 'PC'
        WHEN ca.ca_state IN ('TX', 'OK') THEN 'SO'
        ELSE 'UN'
    END AS region_code
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
""")

# Close the connection
conn.close()

print("ETL process completed. Customer data with US region categorization has been created in table 'customer_with_region'.")