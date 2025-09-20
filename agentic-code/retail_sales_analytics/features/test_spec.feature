Feature: Test Plan for Retail Sales Analytics ETL Pipeline
  As a QA engineer
  I want to validate the retail sales analytics ETL pipeline
  So that I ensure data quality and transformation accuracy

  Background:
    Given I have access to the ETL pipeline for retail sales analytics
    And I have the ability to generate synthetic test data

  Scenario: Generate synthetic data for source tables
    Given I need to create test data for the ETL pipeline
    When I generate synthetic data
    Then I should create the following test datasets:
      | table_name        | record_count | data_characteristics                                                    |
      | store_sales       | 1000         | ss_sold_date_sk: 1-365, ss_item_sk: 1-100, ss_customer_sk: 1-50, ss_store_sk: 1-10, ss_quantity: 1-10, ss_sales_price: 10.00-500.00 |
      | catalog_sales     | 500          | cs_sold_date_sk: 1-365, cs_item_sk: 1-100, cs_bill_customer_sk: 1-50, cs_quantity: 1-5, cs_sales_price: 15.00-300.00 |
      | web_sales         | 750          | ws_sold_date_sk: 1-365, ws_item_sk: 1-100, ws_bill_customer_sk: 1-50, ws_quantity: 1-8, ws_sales_price: 5.00-400.00 |
      | store             | 10           | s_store_sk: 1-10, s_store_id: STORE_001-STORE_010, s_store_name: Store A-J, s_city: Various cities, s_state: Various states |
      | customer          | 50           | c_customer_sk: 1-50, c_customer_id: CUST_001-CUST_050, c_first_name: Random names, c_last_name: Random surnames, c_birth_year: 1950-2000 |
      | item              | 100          | i_item_sk: 1-100, i_item_id: ITEM_001-ITEM_100, i_item_desc: Product descriptions, i_category: Electronics/Clothing/Home, i_brand: Brand A-Z |
      | date_dim          | 365          | d_date_sk: 1-365, d_date: 2023-01-01 to 2023-12-31, d_year: 2023, d_month_seq: 1-12, d_quarter_seq: 1-4 |
      | customer_address  | 50           | ca_address_sk: 1-50, ca_city: Various cities, ca_state: Various states, ca_country: USA |
    And all foreign key relationships should be valid
    And 10% of records should contain null values in optional fields
    And 5% of records should have edge case values

  Scenario: Validate unified sales fact table creation
    Given synthetic source data exists in store_sales, catalog_sales, and web_sales tables
    When the ETL pipeline creates the unified_sales_fact table
    Then I should verify:
      - Total record count equals sum of all source sales tables
      - Each record has a unique sale_id with correct channel prefix
      - Channel values are exactly 'STORE', 'CATALOG', or 'WEB'
      - All sale_date values are valid dates from date_dim table
      - Customer_key values match existing customer surrogate keys
      - Item_key values match existing item surrogate keys
      - Store_key is populated only for STORE channel records
      - Store_key is null for CATALOG and WEB channel records
      - Quantity values are positive integers
      - Unit_price and total_sales values are positive decimals
      - No duplicate sale_id values exist

  Scenario: Validate customer dimension table creation
    Given synthetic source data exists in customer and customer_address tables
    When the ETL pipeline creates the customer_dimension table
    Then I should verify:
      - Record count equals the number of customers in source
      - All customer_key values are unique and match source customer_sk
      - Full_name is properly concatenated from first_name and last_name
      - Birth_year values are within expected range (1900-2010)
      - Address_city and address_state are populated from customer_address
      - No null values exist in required fields
      - Customer records without address have null address fields

  Scenario: Validate item dimension table creation
    Given synthetic source data exists in item table
    When the ETL pipeline creates the item_dimension table
    Then I should verify:
      - Record count equals the number of items in source
      - All item_key values are unique and match source item_sk
      - Item_id values match source exactly
      - Item_description is populated from source item_desc
      - Category and brand values are properly mapped
      - Null values are replaced with 'Unknown' where specified
      - No duplicate item_key values exist

  Scenario: Validate store dimension table creation
    Given synthetic source data exists in store table
    When the ETL pipeline creates the store_dimension table
    Then I should verify:
      - Record count equals or is less than source store count
      - All store_key values are unique and match source store_sk
      - Store_id and store_name are properly mapped
      - City and state values are correctly extracted
      - Market_description is mapped from source market_desc
      - Only active stores are included in the dimension

  Scenario: Validate date dimension table creation
    Given synthetic source data exists in date_dim table
    When the ETL pipeline creates the date_dimension table
    Then I should verify:
      - Record count matches source date_dim within business range
      - All date_key values are unique and match source date_sk
      - Calendar_date values are valid dates
      - Year values match the d_year from source
      - Month values are correctly calculated from month_seq
      - Quarter values are correctly calculated from quarter_seq
      - Date range is within expected business dates

  Scenario: Validate sales summary by channel aggregation
    Given unified_sales_fact table is populated with test data
    When the ETL pipeline creates the sales_summary_by_channel table
    Then I should verify:
      - Each combination of channel and sale_date appears only once
      - Total_quantity equals sum of quantity for each channel/date group
      - Total_sales equals sum of total_sales for each channel/date group
      - Total_profit equals sum of profit for each channel/date group
      - All three channels (STORE, CATALOG, WEB) are represented
      - Records are ordered by sale_date and channel
      - No negative values exist in aggregated fields

  Scenario: Validate data quality and referential integrity
    Given all target tables are populated
    When I perform data quality checks
    Then I should verify:
      - All foreign key relationships are valid
      - No orphaned records exist in fact table
      - Date values are consistent across all tables
      - Numeric values are within expected ranges
      - String fields have appropriate lengths and formats
      - No duplicate primary keys exist in any dimension table

  Scenario: Validate data transformation accuracy with known test cases
    Given I have specific test records with known expected outcomes
    When the ETL pipeline processes these test records
    Then I should verify:
      - Sale_id generation follows the correct pattern
      - Channel mapping is accurate for each source table
      - Date joins produce correct sale_date values
      - Price and quantity calculations are mathematically correct
      - Customer name concatenation handles special characters properly
      - Null value handling follows business rules

  Scenario: Validate performance and scalability
    Given I have generated large volume synthetic datasets
    When the ETL pipeline processes the full dataset
    Then I should verify:
      - Processing completes within acceptable time limits
      - Memory usage remains within specified bounds
      - All transformations complete successfully
      - Target table row counts match expected calculations
      - No performance degradation occurs with increased data volume

  Scenario: Validate error handling and data quality issues
    Given I inject data quality issues into source tables
    When the ETL pipeline processes corrupted data
    Then I should verify:
      - Invalid date values are handled appropriately
      - Null foreign keys are processed according to business rules
      - Duplicate source records are handled correctly
      - Out-of-range numeric values are flagged or corrected
      - Pipeline continues processing valid records
      - Error logs capture all data quality issues