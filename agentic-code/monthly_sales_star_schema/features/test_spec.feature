Feature: Test Plan for Monthly Sales Star Schema ETL
  As a QA engineer
  I want to validate the monthly sales star schema ETL pipeline
  So that I can ensure data accuracy and completeness

  Background: Generate Synthetic Test Data
    Given I generate test data for "store_sales" table with 1000 records:
      | ss_sold_date_sk      | sequential numbers from 2450001 to 2451000     |
      | ss_item_sk           | random numbers between 1 and 100               |
      | ss_store_sk          | random numbers between 1 and 20                |
      | ss_quantity          | random numbers between 1 and 10                |
      | ss_sales_price       | random decimals between 10.00 and 500.00       |
      | ss_ext_sales_price   | ss_quantity * ss_sales_price                    |
      | ss_net_paid          | ss_ext_sales_price * 0.95                      |
      | ss_net_profit        | ss_ext_sales_price * 0.25                      |
    
    And I generate test data for "date_dim" table with 365 records:
      | d_date_sk   | sequential numbers from 2450001 to 2450365 |
      | d_date      | dates from 2023-01-01 to 2023-12-31        |
      | d_year      | 2023 for all records                        |
      | d_moy       | month numbers 1-12 based on d_date          |
      | d_month_seq | sequential numbers from 1 to 12             |
    
    And I generate test data for "item" table with 100 records:
      | i_item_sk     | sequential numbers from 1 to 100                    |
      | i_item_id     | format "ITEM_{i_item_sk}"                           |
      | i_item_desc   | random descriptions like "Quality Product {number}" |
      | i_brand       | random brands from ["Nike", "Adidas", "Puma"]       |
      | i_category    | random categories from ["Sports", "Fashion", "Tech"]|
      | i_class       | random classes from ["Premium", "Standard", "Basic"]|
      | i_product_name| format "Product {i_item_sk} Name"                   |
    
    And I generate test data for "store" table with 20 records:
      | s_store_sk      | sequential numbers from 1 to 20                           |
      | s_store_id      | format "STORE_{s_store_sk}"                               |
      | s_store_name    | format "Store Location {s_store_sk}"                      |
      | s_city          | random cities from ["New York", "Los Angeles", "Chicago"] |
      | s_state         | random states from ["NY", "CA", "IL"]                     |
      | s_market_desc   | random markets from ["Urban", "Suburban", "Rural"]        |
      | s_division_name | random divisions from ["East", "West", "Central"]         |

  Scenario: Validate fact_monthly_sales data completeness
    When the ETL pipeline processes the source data
    Then the "fact_monthly_sales" table should contain records
    And all records should have non-null values for date_key, item_key, and store_key
    And total_quantity should equal the sum of ss_quantity grouped by year, month, item, and store
    And total_sales_amount should equal the sum of ss_ext_sales_price grouped by year, month, item, and store
    And total_net_paid should equal the sum of ss_net_paid grouped by year, month, item, and store
    And total_net_profit should equal the sum of ss_net_profit grouped by year, month, item, and store

  Scenario: Validate fact_monthly_sales aggregation logic
    Given store_sales records with same ss_item_sk "1", ss_store_sk "1", and dates in "2023-01"
    When the ETL pipeline aggregates the data
    Then there should be exactly one record in fact_monthly_sales for item_key "1", store_key "1", and sales_month "1"
    And the total_quantity should equal the sum of all ss_quantity for those records
    And the total_sales_amount should equal the sum of all ss_ext_sales_price for those records

  Scenario: Validate fact_monthly_sales join integrity
    Given store_sales records with ss_sold_date_sk that exist in date_dim
    When the ETL pipeline processes the data
    Then all records in fact_monthly_sales should have valid date_key values
    And sales_year should match d_year from the corresponding date_dim record
    And sales_month should match d_moy from the corresponding date_dim record

  Scenario: Validate dim_item data accuracy
    When the ETL pipeline creates the dim_item table
    Then dim_item should contain exactly 100 records
    And each item_key should correspond to an i_item_sk from the source
    And item_id should match i_item_id from the source
    And brand_name should match i_brand from the source
    And category_name should match i_category from the source
    And class_name should match i_class from the source
    And product_name should match i_product_name from the source

  Scenario: Validate dim_store data accuracy
    When the ETL pipeline creates the dim_store table
    Then dim_store should contain exactly 20 records
    And each store_key should correspond to an s_store_sk from the source
    And store_id should match s_store_id from the source
    And store_name should match s_store_name from the source
    And city should match s_city from the source
    And state should match s_state from the source
    And market_desc should match s_market_desc from the source
    And division_name should match s_division_name from the source

  Scenario: Validate dim_date data accuracy
    When the ETL pipeline creates the dim_date table
    Then dim_date should contain exactly 365 records
    And each date_key should correspond to a d_date_sk from the source
    And full_date should match d_date from the source
    And year should match d_year from the source
    And month should match d_moy from the source
    And month_seq should match d_month_seq from the source

  Scenario: Validate referential integrity between fact and dimension tables
    When the ETL pipeline completes
    Then every date_key in fact_monthly_sales should exist in dim_date
    And every item_key in fact_monthly_sales should exist in dim_item
    And every store_key in fact_monthly_sales should exist in dim_store

  Scenario: Validate data types and constraints
    When the ETL pipeline completes
    Then all numeric columns in fact_monthly_sales should contain valid numbers
    And all date columns in dim_date should contain valid dates
    And all string columns should not exceed their defined lengths
    And all key columns should contain non-null values

  Scenario: Validate monthly aggregation boundaries
    Given store_sales data spanning multiple months
    When the ETL pipeline processes the data
    Then records from different months should be aggregated separately
    And January data should not be mixed with February data
    And each month should have separate records in fact_monthly_sales

  Scenario: Validate empty source data handling
    Given empty source tables
    When the ETL pipeline processes the data
    Then fact_monthly_sales should be empty
    And dim_item should be empty
    And dim_store should be empty
    And dim_date should be empty
    And no errors should occur during processing