import duckdb

# Connect to persistent DuckDB database
conn = duckdb.connect('./data/my_tpc.db')

try:
    # Drop target table if it exists
    conn.execute("DROP TABLE IF EXISTS sales_fact")
    
    # Create the unified sales_fact table
    conn.execute("""
    CREATE TABLE sales_fact AS
    WITH store_sales_transformed AS (
        SELECT 
            'STORE_' || CAST(ss_ticket_number AS VARCHAR) as sale_id,
            d.d_date as sale_date,
            c.c_customer_id as customer_id,
            i.i_item_id as item_id,
            s.s_store_id as store_id,
            'STORE' as channel,
            ss.ss_quantity as quantity,
            ss.ss_sales_price as sales_amount,
            ss.ss_net_profit as profit_amount,
            d.d_year as year,
            d.d_quarter as quarter,
            d.d_month as month,
            c.c_first_name || ' ' || c.c_last_name as customer_name,
            i.i_item_desc as item_description,
            i.i_category as item_category,
            s.s_store_name as store_name,
            s.s_city || ', ' || s.s_state as store_location
        FROM store_sales ss
        INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        LEFT JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        LEFT JOIN item i ON ss.ss_item_sk = i.i_item_sk
        LEFT JOIN store s ON ss.ss_store_sk = s.s_store_sk
    ),
    
    catalog_sales_transformed AS (
        SELECT 
            'CATALOG_' || CAST(cs_order_number AS VARCHAR) as sale_id,
            d.d_date as sale_date,
            c.c_customer_id as customer_id,
            i.i_item_id as item_id,
            NULL as store_id,
            'CATALOG' as channel,
            cs.cs_quantity as quantity,
            cs.cs_sales_price as sales_amount,
            cs.cs_net_profit as profit_amount,
            d.d_year as year,
            d.d_quarter as quarter,
            d.d_month as month,
            c.c_first_name || ' ' || c.c_last_name as customer_name,
            i.i_item_desc as item_description,
            i.i_category as item_category,
            NULL as store_name,
            NULL as store_location
        FROM catalog_sales cs
        INNER JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
        LEFT JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
        LEFT JOIN item i ON cs.cs_item_sk = i.i_item_sk
    ),
    
    web_sales_transformed AS (
        SELECT 
            'WEB_' || CAST(ws_order_number AS VARCHAR) as sale_id,
            d.d_date as sale_date,
            c.c_customer_id as customer_id,
            i.i_item_id as item_id,
            NULL as store_id,
            'WEB' as channel,
            ws.ws_quantity as quantity,
            ws.ws_sales_price as sales_amount,
            ws.ws_net_profit as profit_amount,
            d.d_year as year,
            d.d_quarter as quarter,
            d.d_month as month,
            c.c_first_name || ' ' || c.c_last_name as customer_name,
            i.i_item_desc as item_description,
            i.i_category as item_category,
            NULL as store_name,
            NULL as store_location
        FROM web_sales ws
        INNER JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
        LEFT JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
        LEFT JOIN item i ON ws.ws_item_sk = i.i_item_sk
    )
    
    SELECT *
    FROM store_sales_transformed
    WHERE sale_date IS NOT NULL AND quantity >= 0
    
    UNION ALL
    
    SELECT *
    FROM catalog_sales_transformed
    WHERE sale_date IS NOT NULL AND quantity >= 0
    
    UNION ALL
    
    SELECT *
    FROM web_sales_transformed
    WHERE sale_date IS NOT NULL AND quantity >= 0
    
    ORDER BY sale_date, channel
    """)
    
    # Verify the results
    result = conn.execute("SELECT COUNT(*) as total_records FROM sales_fact").fetchone()
    print(f"Total records in sales_fact table: {result[0]}")
    
    # Show sample data
    sample = conn.execute("""
        SELECT channel, COUNT(*) as record_count 
        FROM sales_fact 
        GROUP BY channel 
        ORDER BY channel
    """).fetchall()
    
    for row in sample:
        print(f"Channel {row[0]}: {row[1]} records")

finally:
    conn.close()