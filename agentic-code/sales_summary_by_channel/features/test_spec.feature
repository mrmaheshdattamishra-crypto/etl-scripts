Feature: Sales Summary By Channel ETL Test Plan

Background:
  Given I have access to ETL pipeline for sales_summary_by_channel

Scenario: Generate Synthetic Data for Source Tables
  Given I generate test data for "date_dim" table:
    | d_date_sk | d_date     | d_year | d_moy | d_qoy | d_day_name |
    | 1         | 2023-01-15 | 2023   | 1     | 1     | Sunday     |
    | 2         | 2023-02-20 | 2023   | 2     | 1     | Monday     |
    | 3         | 2023-03-10 | 2023   | 3     | 1     | Friday     |
    | 4         | 2023-04-05 | 2023   | 4     | 2     | Wednesday  |
    | 5         | 2023-05-12 | 2023   | 5     | 2     | Friday     |

  And I generate test data for "item" table:
    | i_item_sk | i_item_id | i_category   | i_brand | i_class  |
    | 100       | ITEM001   | Electronics  | Apple   | Premium  |
    | 101       | ITEM002   | Clothing     | Nike    | Sports   |
    | 102       | ITEM003   | Books        | Amazon  | Fiction  |
    | 103       | ITEM004   | Electronics  | Samsung | Standard |
    | 104       | ITEM005   | Home         | IKEA    | Modern   |

  And I generate test data for "store_sales" table:
    | ss_sold_date_sk | ss_item_sk | ss_quantity | ss_sales_price | ss_ext_sales_price | ss_net_profit | ss_store_sk |
    | 1               | 100        | 2           | 500.00         | 1000.00            | 200.00        | 1           |
    | 2               | 101        | 1           | 80.00          | 80.00              | 20.00         | 1           |
    | 3               | 102        | 3           | 15.00          | 45.00              | 10.00         | 2           |
    | 4               | 103        | 1           | 300.00         | 300.00             | 50.00         | 1           |
    | 5               | 104        | 2           | 120.00         | 240.00             | 40.00         | 2           |

  And I generate test data for "catalog_sales" table:
    | cs_sold_date_sk | cs_item_sk | cs_quantity | cs_sales_price | cs_ext_sales_price | cs_net_profit | cs_call_center_sk |
    | 1               | 100        | 1           | 500.00         | 500.00             | 100.00        | 1                 |
    | 2               | 102        | 2           | 15.00          | 30.00              | 8.00          | 1                 |
    | 3               | 103        | 1           | 300.00         | 300.00             | 60.00         | 2                 |
    | 4               | 104        | 3           | 120.00         | 360.00             | 80.00         | 1                 |

  And I generate test data for "web_sales" table:
    | ws_sold_date_sk | ws_item_sk | ws_quantity | ws_sales_price | ws_ext_sales_price | ws_net_profit | ws_web_site_sk |
    | 1               | 101        | 2           | 80.00          | 160.00             | 30.00         | 1              |
    | 2               | 102        | 1           | 15.00          | 15.00              | 5.00          | 1              |
    | 3               | 100        | 1           | 500.00         | 500.00             | 120.00        | 2              |
    | 4               | 104        | 2           | 120.00         | 240.00             | 50.00         | 1              |
    | 5               | 103        | 1           | 300.00         | 300.00             | 70.00         | 2              |

Scenario: Test Basic ETL Execution
  Given I have loaded synthetic data into source tables
  When I execute the sales_summary_by_channel ETL pipeline
  Then the ETL should complete successfully
  And the target table "sales_summary_by_channel" should contain data

Scenario: Test Date Dimension Mapping
  Given the ETL has been executed with synthetic data
  When I query the target table for date-related fields
  Then sale_date should match d_date from date_dim table
  And year should match d_year from date_dim table
  And month should match d_moy from date_dim table
  And quarter should match d_qoy from date_dim table
  And day_name should match d_day_name from date_dim table

Scenario: Test Channel Classification
  Given the ETL has been executed with synthetic data
  When I query the target table for channel field
  Then records from store_sales should have channel = "store"
  And records from catalog_sales should have channel = "catalog"
  And records from web_sales should have channel = "web"
  And no records should have null or empty channel values

Scenario: Test Item Dimension Mapping
  Given the ETL has been executed with synthetic data
  When I query the target table for item-related fields
  Then item_category should match i_category from item table
  And item_brand should match i_brand from item table
  And item_class should match i_class from item table

Scenario: Test Quantity Aggregation
  Given the ETL has been executed with synthetic data
  When I query total_quantity for a specific date, channel, and item combination
  Then total_quantity should equal the sum of quantities from the respective sales table
  And total_quantity should be greater than 0 for all records

Scenario: Test Sales Amount Aggregation
  Given the ETL has been executed with synthetic data
  When I query total_sales_amount for a specific grouping
  Then total_sales_amount should equal the sum of ext_sales_price from the respective sales table
  And total_sales_amount should be greater than 0 for all records

Scenario: Test Net Profit Aggregation
  Given the ETL has been executed with synthetic data
  When I query total_net_profit for a specific grouping
  Then total_net_profit should equal the sum of net_profit from the respective sales table

Scenario: Test Average Unit Price Calculation
  Given the ETL has been executed with synthetic data
  When I query avg_unit_price for any record
  Then avg_unit_price should equal total_sales_amount divided by total_quantity
  And avg_unit_price should not be null for any record with total_quantity > 0

Scenario: Test Transaction Count
  Given the ETL has been executed with synthetic data
  When I query transaction_count for a specific grouping
  Then transaction_count should equal the count of distinct transactions for that grouping
  And transaction_count should be greater than 0 for all records

Scenario: Test Grouping Logic
  Given the ETL has been executed with synthetic data
  When I examine the target table structure
  Then records should be grouped by sale_date, channel, item_category, item_brand, and item_class
  And no duplicate combinations of these grouping fields should exist

Scenario: Test Data Completeness
  Given the ETL has been executed with synthetic data
  When I compare source and target record counts
  Then all valid source records with matching date and item keys should be represented in target
  And no source records should be lost due to invalid joins

Scenario: Test Null Value Handling
  Given I generate test data with null values in optional fields
  When I execute the ETL pipeline
  Then the pipeline should handle null values gracefully
  And critical fields like sale_date, channel, and totals should not be null

Scenario: Test Data Type Consistency
  Given the ETL has been executed with synthetic data
  When I examine the target table data types
  Then all numeric fields should contain valid numbers
  And all date fields should contain valid dates
  And all string fields should contain valid text values

Scenario: Test Edge Case - Zero Quantity
  Given I generate test data with zero quantity transactions
  When I execute the ETL pipeline
  Then records with zero quantity should be handled appropriately
  And avg_unit_price calculation should handle division by zero

Scenario: Test Edge Case - Large Numbers
  Given I generate test data with large numeric values
  When I execute the ETL pipeline
  Then the pipeline should handle large numbers without overflow
  And calculations should maintain precision

Scenario: Test Performance with Volume
  Given I generate 10000 records across all source tables
  When I execute the ETL pipeline
  Then the pipeline should complete within acceptable time limits
  And memory usage should remain within bounds