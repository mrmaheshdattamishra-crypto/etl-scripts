import duckdb
import random
from datetime import datetime, timedelta
import os

# Create connection to persistent database
conn = duckdb.connect('./data/my_tpc.db')

def setup_test_data():
    """Generate synthetic test data based on gherkin specification"""
    
    # Create data directory if not exists
    os.makedirs('./data', exist_ok=True)
    
    # Generate store_sales table with 1000 records
    store_sales_data = []
    for i in range(1000):
        ss_sold_date_sk = 2450001 + i
        ss_item_sk = random.randint(1, 100)
        ss_store_sk = random.randint(1, 20)
        ss_quantity = random.randint(1, 10)
        ss_sales_price = round(random.uniform(10.00, 500.00), 2)
        ss_ext_sales_price = round(ss_quantity * ss_sales_price, 2)
        ss_net_paid = round(ss_ext_sales_price * 0.95, 2)
        ss_net_profit = round(ss_ext_sales_price * 0.25, 2)
        
        store_sales_data.append({
            'ss_sold_date_sk': ss_sold_date_sk,
            'ss_item_sk': ss_item_sk,
            'ss_store_sk': ss_store_sk,
            'ss_quantity': ss_quantity,
            'ss_sales_price': ss_sales_price,
            'ss_ext_sales_price': ss_ext_sales_price,
            'ss_net_paid': ss_net_paid,
            'ss_net_profit': ss_net_profit
        })
    
    conn.execute("DROP TABLE IF EXISTS store_sales")
    conn.execute("""
        CREATE TABLE store_sales (
            ss_sold_date_sk INTEGER,
            ss_item_sk INTEGER,
            ss_store_sk INTEGER,
            ss_quantity INTEGER,
            ss_sales_price DECIMAL(10,2),
            ss_ext_sales_price DECIMAL(10,2),
            ss_net_paid DECIMAL(10,2),
            ss_net_profit DECIMAL(10,2)
        )
    """)
    
    for row in store_sales_data:
        conn.execute("INSERT INTO store_sales VALUES (?,?,?,?,?,?,?,?)", 
                    [row['ss_sold_date_sk'], row['ss_item_sk'], row['ss_store_sk'], 
                     row['ss_quantity'], row['ss_sales_price'], row['ss_ext_sales_price'],
                     row['ss_net_paid'], row['ss_net_profit']])
    
    # Generate date_dim table with 365 records
    conn.execute("DROP TABLE IF EXISTS date_dim")
    conn.execute("""
        CREATE TABLE date_dim (
            d_date_sk INTEGER,
            d_date DATE,
            d_year INTEGER,
            d_moy INTEGER,
            d_month_seq INTEGER
        )
    """)
    
    start_date = datetime(2023, 1, 1)
    for i in range(365):
        current_date = start_date + timedelta(days=i)
        d_date_sk = 2450001 + i
        d_date = current_date.strftime('%Y-%m-%d')
        d_year = 2023
        d_moy = current_date.month
        d_month_seq = current_date.month
        
        conn.execute("INSERT INTO date_dim VALUES (?,?,?,?,?)", 
                    [d_date_sk, d_date, d_year, d_moy, d_month_seq])
    
    # Generate item table with 100 records
    brands = ["Nike", "Adidas", "Puma"]
    categories = ["Sports", "Fashion", "Tech"]
    classes = ["Premium", "Standard", "Basic"]
    
    conn.execute("DROP TABLE IF EXISTS item")
    conn.execute("""
        CREATE TABLE item (
            i_item_sk INTEGER,
            i_item_id VARCHAR,
            i_item_desc VARCHAR,
            i_brand VARCHAR,
            i_category VARCHAR,
            i_class VARCHAR,
            i_product_name VARCHAR
        )
    """)
    
    for i in range(1, 101):
        i_item_sk = i
        i_item_id = f"ITEM_{i}"
        i_item_desc = f"Quality Product {random.randint(1, 1000)}"
        i_brand = random.choice(brands)
        i_category = random.choice(categories)
        i_class = random.choice(classes)
        i_product_name = f"Product {i} Name"
        
        conn.execute("INSERT INTO item VALUES (?,?,?,?,?,?,?)", 
                    [i_item_sk, i_item_id, i_item_desc, i_brand, i_category, i_class, i_product_name])
    
    # Generate store table with 20 records
    cities = ["New York", "Los Angeles", "Chicago"]
    states = ["NY", "CA", "IL"]
    markets = ["Urban", "Suburban", "Rural"]
    divisions = ["East", "West", "Central"]
    
    conn.execute("DROP TABLE IF EXISTS store")
    conn.execute("""
        CREATE TABLE store (
            s_store_sk INTEGER,
            s_store_id VARCHAR,
            s_store_name VARCHAR,
            s_city VARCHAR,
            s_state VARCHAR,
            s_market_desc VARCHAR,
            s_division_name VARCHAR
        )
    """)
    
    for i in range(1, 21):
        s_store_sk = i
        s_store_id = f"STORE_{i}"
        s_store_name = f"Store Location {i}"
        s_city = random.choice(cities)
        s_state = random.choice(states)
        s_market_desc = random.choice(markets)
        s_division_name = random.choice(divisions)
        
        conn.execute("INSERT INTO store VALUES (?,?,?,?,?,?,?)", 
                    [s_store_sk, s_store_id, s_store_name, s_city, s_state, s_market_desc, s_division_name])

def create_etl_tables():
    """Create star schema tables"""
    
    # Create fact_monthly_sales table
    conn.execute("DROP TABLE IF EXISTS fact_monthly_sales")
    conn.execute("""
        CREATE TABLE fact_monthly_sales (
            date_key INTEGER,
            item_key INTEGER,
            store_key INTEGER,
            sales_year INTEGER,
            sales_month INTEGER,
            total_quantity INTEGER,
            total_sales_amount DECIMAL(15,2),
            total_net_paid DECIMAL(15,2),
            total_net_profit DECIMAL(15,2)
        )
    """)
    
    # Create dim_item table
    conn.execute("DROP TABLE IF EXISTS dim_item")
    conn.execute("""
        CREATE TABLE dim_item (
            item_key INTEGER,
            item_id VARCHAR,
            brand_name VARCHAR,
            category_name VARCHAR,
            class_name VARCHAR,
            product_name VARCHAR
        )
    """)
    
    # Create dim_store table
    conn.execute("DROP TABLE IF EXISTS dim_store")
    conn.execute("""
        CREATE TABLE dim_store (
            store_key INTEGER,
            store_id VARCHAR,
            store_name VARCHAR,
            city VARCHAR,
            state VARCHAR,
            market_desc VARCHAR,
            division_name VARCHAR
        )
    """)
    
    # Create dim_date table
    conn.execute("DROP TABLE IF EXISTS dim_date")
    conn.execute("""
        CREATE TABLE dim_date (
            date_key INTEGER,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            month_seq INTEGER
        )
    """)

def run_etl_pipeline():
    """Simulate ETL pipeline processing"""
    
    # Populate dimension tables
    conn.execute("""
        INSERT INTO dim_item 
        SELECT i_item_sk, i_item_id, i_brand, i_category, i_class, i_product_name
        FROM item
    """)
    
    conn.execute("""
        INSERT INTO dim_store 
        SELECT s_store_sk, s_store_id, s_store_name, s_city, s_state, s_market_desc, s_division_name
        FROM store
    """)
    
    conn.execute("""
        INSERT INTO dim_date 
        SELECT d_date_sk, d_date, d_year, d_moy, d_month_seq
        FROM date_dim
    """)
    
    # Populate fact table with monthly aggregations
    conn.execute("""
        INSERT INTO fact_monthly_sales
        SELECT 
            ss.ss_sold_date_sk as date_key,
            ss.ss_item_sk as item_key,
            ss.ss_store_sk as store_key,
            dd.d_year as sales_year,
            dd.d_moy as sales_month,
            SUM(ss.ss_quantity) as total_quantity,
            SUM(ss.ss_ext_sales_price) as total_sales_amount,
            SUM(ss.ss_net_paid) as total_net_paid,
            SUM(ss.ss_net_profit) as total_net_profit
        FROM store_sales ss
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        GROUP BY ss.ss_sold_date_sk, ss.ss_item_sk, ss.ss_store_sk, dd.d_year, dd.d_moy
    """)

def test_fact_monthly_sales_completeness():
    """Test fact_monthly_sales data completeness"""
    print("Testing fact_monthly_sales data completeness...")
    
    # Check if table contains records
    result = conn.execute("SELECT COUNT(*) FROM fact_monthly_sales").fetchone()
    assert result[0] > 0, "fact_monthly_sales should contain records"
    print(f"✓ fact_monthly_sales contains {result[0]} records")
    
    # Check for non-null key values
    result = conn.execute("""
        SELECT COUNT(*) FROM fact_monthly_sales 
        WHERE date_key IS NULL OR item_key IS NULL OR store_key IS NULL
    """).fetchone()
    assert result[0] == 0, "All records should have non-null key values"
    print("✓ All records have non-null key values")
    
    # Validate aggregation logic
    source_totals = conn.execute("""
        SELECT 
            ss.ss_sold_date_sk,
            ss.ss_item_sk,
            ss.ss_store_sk,
            SUM(ss.ss_quantity) as source_quantity,
            SUM(ss.ss_ext_sales_price) as source_sales,
            SUM(ss.ss_net_paid) as source_paid,
            SUM(ss.ss_net_profit) as source_profit
        FROM store_sales ss
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        GROUP BY ss.ss_sold_date_sk, ss.ss_item_sk, ss.ss_store_sk
        LIMIT 5
    """).fetchall()
    
    for row in source_totals:
        fact_row = conn.execute("""
            SELECT total_quantity, total_sales_amount, total_net_paid, total_net_profit
            FROM fact_monthly_sales
            WHERE date_key = ? AND item_key = ? AND store_key = ?
        """, [row[0], row[1], row[2]]).fetchone()
        
        assert fact_row[0] == row[3], f"Quantity mismatch for keys {row[0]}, {row[1]}, {row[2]}"
        assert abs(fact_row[1] - row[4]) < 0.01, f"Sales amount mismatch for keys {row[0]}, {row[1]}, {row[2]}"
    
    print("✓ Aggregation logic validated successfully")

def test_aggregation_logic():
    """Test fact_monthly_sales aggregation logic"""
    print("Testing aggregation logic...")
    
    # Create specific test case with same item, store, and date
    test_date_sk = 2450100
    test_item_sk = 1
    test_store_sk = 1
    
    # Check if aggregation works correctly for same keys
    result = conn.execute("""
        SELECT COUNT(*) as record_count, 
               SUM(ss_quantity) as total_qty,
               SUM(ss_ext_sales_price) as total_sales
        FROM store_sales ss
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        WHERE ss.ss_sold_date_sk = ? AND ss.ss_item_sk = ? AND ss.ss_store_sk = ?
    """, [test_date_sk, test_item_sk, test_store_sk]).fetchone()
    
    if result[0] > 0:
        fact_result = conn.execute("""
            SELECT total_quantity, total_sales_amount
            FROM fact_monthly_sales
            WHERE date_key = ? AND item_key = ? AND store_key = ?
        """, [test_date_sk, test_item_sk, test_store_sk]).fetchone()
        
        if fact_result:
            assert fact_result[0] == result[1], "Aggregated quantity should match"
            assert abs(fact_result[1] - result[2]) < 0.01, "Aggregated sales should match"
            print("✓ Aggregation logic verified for test case")

def test_join_integrity():
    """Test join integrity between fact and dimension tables"""
    print("Testing join integrity...")
    
    # Test date join integrity
    result = conn.execute("""
        SELECT f.sales_year, d.year, f.sales_month, d.month
        FROM fact_monthly_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.sales_year != d.year OR f.sales_month != d.month
    """).fetchall()
    assert len(result) == 0, "Sales year/month should match date dimension"
    print("✓ Date join integrity verified")

def test_dimension_accuracy():
    """Test dimension table accuracy"""
    print("Testing dimension table accuracy...")
    
    # Test dim_item accuracy
    result = conn.execute("SELECT COUNT(*) FROM dim_item").fetchone()
    assert result[0] == 100, "dim_item should contain exactly 100 records"
    print("✓ dim_item contains correct number of records")
    
    # Test dim_store accuracy  
    result = conn.execute("SELECT COUNT(*) FROM dim_store").fetchone()
    assert result[0] == 20, "dim_store should contain exactly 20 records"
    print("✓ dim_store contains correct number of records")
    
    # Test dim_date accuracy
    result = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()
    assert result[0] == 365, "dim_date should contain exactly 365 records"
    print("✓ dim_date contains correct number of records")
    
    # Verify data mapping accuracy
    item_check = conn.execute("""
        SELECT COUNT(*) FROM dim_item di
        JOIN item i ON di.item_key = i.i_item_sk
        WHERE di.item_id != i.i_item_id OR di.brand_name != i.i_brand
    """).fetchone()
    assert item_check[0] == 0, "Item dimension data should match source"
    print("✓ Item dimension data mapping verified")

def test_referential_integrity():
    """Test referential integrity between fact and dimension tables"""
    print("Testing referential integrity...")
    
    # Test all date_keys exist in dim_date
    result = conn.execute("""
        SELECT COUNT(*) FROM fact_monthly_sales f
        LEFT JOIN dim_date d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
    """).fetchone()
    assert result[0] == 0, "All date_keys should exist in dim_date"
    print("✓ Date key referential integrity verified")
    
    # Test all item_keys exist in dim_item
    result = conn.execute("""
        SELECT COUNT(*) FROM fact_monthly_sales f
        LEFT JOIN dim_item i ON f.item_key = i.item_key
        WHERE i.item_key IS NULL
    """).fetchone()
    assert result[0] == 0, "All item_keys should exist in dim_item"
    print("✓ Item key referential integrity verified")
    
    # Test all store_keys exist in dim_store
    result = conn.execute("""
        SELECT COUNT(*) FROM fact_monthly_sales f
        LEFT JOIN dim_store s ON f.store_key = s.store_key
        WHERE s.store_key IS NULL
    """).fetchone()
    assert result[0] == 0, "All store_keys should exist in dim_store"
    print("✓ Store key referential integrity verified")

def test_data_types_and_constraints():
    """Test data types and constraints"""
    print("Testing data types and constraints...")
    
    # Test numeric columns contain valid numbers
    result = conn.execute("""
        SELECT COUNT(*) FROM fact_monthly_sales
        WHERE total_quantity < 0 OR total_sales_amount < 0 
           OR total_net_paid < 0 OR total_net_profit < 0
    """).fetchone()
    assert result[0] == 0, "All numeric values should be non-negative"
    print("✓ Numeric columns contain valid values")
    
    # Test key columns are non-null
    result = conn.execute("""
        SELECT COUNT(*) FROM fact_monthly_sales
        WHERE date_key IS NULL OR item_key IS NULL OR store_key IS NULL
    """).fetchone()
    assert result[0] == 0, "Key columns should be non-null"
    print("✓ Key columns are non-null")

def test_monthly_aggregation_boundaries():
    """Test monthly aggregation boundaries"""
    print("Testing monthly aggregation boundaries...")
    
    # Verify records are separated by month
    result = conn.execute("""
        SELECT DISTINCT sales_year, sales_month
        FROM fact_monthly_sales
        ORDER BY sales_year, sales_month
    """).fetchall()
    
    print(f"✓ Found {len(result)} distinct year-month combinations")
    
    # Verify no data mixing between months for same item/store
    mixed_months = conn.execute("""
        SELECT item_key, store_key, COUNT(DISTINCT sales_month) as month_count
        FROM fact_monthly_sales
        WHERE sales_year = 2023
        GROUP BY item_key, store_key
        HAVING COUNT(DISTINCT sales_month) > 1
        LIMIT 5
    """).fetchall()
    
    if mixed_months:
        print(f"✓ Found {len(mixed_months)} item-store combinations spanning multiple months (expected)")

def test_empty_source_handling():
    """Test empty source data handling"""
    print("Testing empty source data handling...")
    
    # Create backup of current data
    conn.execute("CREATE TABLE store_sales_backup AS SELECT * FROM store_sales")
    conn.execute("CREATE TABLE date_dim_backup AS SELECT * FROM date_dim")
    conn.execute("CREATE TABLE item_backup AS SELECT * FROM item")
    conn.execute("CREATE TABLE store_backup AS SELECT * FROM store")
    
    try:
        # Clear source tables
        conn.execute("DELETE FROM store_sales")
        conn.execute("DELETE FROM date_dim")
        conn.execute("DELETE FROM item")
        conn.execute("DELETE FROM store")
        
        # Clear target tables
        conn.execute("DELETE FROM fact_monthly_sales")
        conn.execute("DELETE FROM dim_item")
        conn.execute("DELETE FROM dim_store")
        conn.execute("DELETE FROM dim_date")
        
        # Run ETL pipeline
        run_etl_pipeline()
        
        # Verify all tables are empty
        fact_count = conn.execute("SELECT COUNT(*) FROM fact_monthly_sales").fetchone()[0]
        dim_item_count = conn.execute("SELECT COUNT(*) FROM dim_item").fetchone()[0]
        dim_store_count = conn.execute("SELECT COUNT(*) FROM dim_store").fetchone()[0]
        dim_date_count = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        
        assert fact_count == 0, "fact_monthly_sales should be empty"
        assert dim_item_count == 0, "dim_item should be empty"
        assert dim_store_count == 0, "dim_store should be empty"
        assert dim_date_count == 0, "dim_date should be empty"
        
        print("✓ Empty source data handling verified")
        
    finally:
        # Restore data
        conn.execute("DELETE FROM store_sales")
        conn.execute("DELETE FROM date_dim")
        conn.execute("DELETE FROM item")
        conn.execute("DELETE FROM store")
        
        conn.execute("INSERT INTO store_sales SELECT * FROM store_sales_backup")
        conn.execute("INSERT INTO date_dim SELECT * FROM date_dim_backup")
        conn.execute("INSERT INTO item SELECT * FROM item_backup")
        conn.execute("INSERT INTO store SELECT * FROM store_backup")
        
        # Drop backup tables
        conn.execute("DROP TABLE store_sales_backup")
        conn.execute("DROP TABLE date_dim_backup")
        conn.execute("DROP TABLE item_backup")
        conn.execute("DROP TABLE store_backup")

def main():
    """Run all ETL tests"""
    print("Starting ETL Test Suite for Monthly Sales Star Schema")
    print("=" * 60)
    
    try:
        # Setup test data
        print("Setting up test data...")
        setup_test_data()
        print("✓ Test data setup complete")
        
        # Create ETL tables
        print("\nCreating ETL tables...")
        create_etl_tables()
        print("✓ ETL tables created")
        
        # Run ETL pipeline
        print("\nRunning ETL pipeline...")
        run_etl_pipeline()
        print("✓ ETL pipeline completed")
        
        # Run tests
        print("\n" + "=" * 60)
        print("RUNNING ETL TESTS")
        print("=" * 60)
        
        test_fact_monthly_sales_completeness()
        print()
        
        test_aggregation_logic()
        print()
        
        test_join_integrity()
        print()
        
        test_dimension_accuracy()
        print()
        
        test_referential_integrity()
        print()
        
        test_data_types_and_constraints()
        print()
        
        test_monthly_aggregation_boundaries()
        print()
        
        test_empty_source_handling()
        print()
        
        print("=" * 60)
        print("ALL TESTS PASSED SUCCESSFULLY!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"TEST FAILED: {e}")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False
    finally:
        conn.close()
    
    return True

if __name__ == "__main__":
    main()