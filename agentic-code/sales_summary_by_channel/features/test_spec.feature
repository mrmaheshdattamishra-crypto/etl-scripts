Feature: Test Plan for sales_summary_by_channel ETL Pipeline

Background:
    Given synthetic data generation is configured for all source tables

Scenario: Generate synthetic data for source tables
    Given I need to generate test data for "store_sales" table with 1000 records
        | ss_sold_date_sk    | random_number_between(1, 3650)     |
        | ss_item_sk         | random_number_between(1, 50000)    |
        | ss_customer_sk     | random_number_between(1, 100000)   |
        | ss_store_sk        | random_number_between(1, 100)      |
        | ss_quantity        | random_number_between(1, 10)       |
        | ss_sales_price     | random_decimal_between(5.0, 500.0) |
        | ss_ext_sales_price | ss_quantity * ss_sales_price       |
        | ss_net_paid        | ss_ext_sales_price * 0.95          |
        | ss_net_profit      | ss_ext_sales_price * 0.2           |

    And I need to generate test data for "web_sales" table with 800 records
        | ws_sold_date_sk     | random_number_between(1, 3650)     |
        | ws_item_sk          | random_number_between(1, 50000)    |
        | ws_bill_customer_sk | random_number_between(1, 100000)   |
        | ws_web_site_sk      | random_number_between(1, 50)       |
        | ws_quantity         | random_number_between(1, 8)        |
        | ws_sales_price      | random_decimal_between(10.0, 300.0)|
        | ws_ext_sales_price  | ws_quantity * ws_sales_price       |
        | ws_net_paid         | ws_ext_sales_price * 0.93          |
        | ws_net_profit       | ws_ext_sales_price * 0.18          |

    And I need to generate test data for "catalog_sales" table with 600 records
        | cs_sold_date_sk     | random_number_between(1, 3650)     |
        | cs_item_sk          | random_number_between(1, 50000)    |
        | cs_bill_customer_sk | random_number_between(1, 100000)   |
        | cs_call_center_sk   | random_number_between(1, 20)       |
        | cs_quantity         | random_number_between(1, 5)        |
        | cs_sales_price      | random_decimal_between(15.0, 400.0)|
        | cs_ext_sales_price  | cs_quantity * cs_sales_price       |
        | cs_net_paid         | cs_ext_sales_price * 0.92          |
        | cs_net_profit       | cs_ext_sales_price * 0.15          |

    And I need to generate test data for "date_dim" table with 3650 records
        | d_date_sk      | sequence_number_starting_from(1)         |
        | d_date         | date_sequence_starting_from('2014-01-01')|
        | d_year         | extract_year_from(d_date)                |
        | d_moy          | extract_month_from(d_date)               |
        | d_qoy          | extract_quarter_from(d_date)             |
        | d_day_name     | extract_day_name_from(d_date)            |
        | d_quarter_name | concat('Q', d_qoy)                       |

    And I need to generate test data for "store" table with 100 records
        | s_store_sk   | sequence_number_starting_from(1)    |
        | s_store_name | concat('Store_', s_store_sk)        |
        | s_state      | random_state_code()                 |
        | s_city       | random_city_name()                  |

    And I need to generate test data for "web_site" table with 50 records
        | web_site_sk | sequence_number_starting_from(1)    |
        | web_name    | concat('Website_', web_site_sk)     |
        | web_state   | random_state_code()                 |
        | web_city    | random_city_name()                  |

    And I need to generate test data for "call_center" table with 20 records
        | cc_call_center_sk | sequence_number_starting_from(1)        |
        | cc_name           | concat('CallCenter_', cc_call_center_sk)|
        | cc_state          | random_state_code()                     |
        | cc_city           | random_city_name()                      |

Scenario: Test store sales data transformation and aggregation
    Given the source tables are populated with test data
    When the ETL pipeline processes store sales data
    Then the output should contain records with channel = 'store'
    And each record should have valid joins between store_sales, date_dim, and store tables
    And sales_date should match d_date from date_dim table
    And year should match d_year from date_dim table
    And month should match d_moy from date_dim table
    And quarter should match d_qoy from date_dim table
    And day_name should match d_day_name from date_dim table
    And channel_location should match s_store_name from store table
    And channel_state should match s_state from store table
    And total_quantity should equal sum of ss_quantity grouped by date and store
    And total_sales_amount should equal sum of ss_ext_sales_price grouped by date and store
    And total_net_paid should equal sum of ss_net_paid grouped by date and store
    And total_net_profit should equal sum of ss_net_profit grouped by date and store
    And avg_sales_price should equal total_sales_amount divided by total_quantity

Scenario: Test web sales data transformation and aggregation
    Given the source tables are populated with test data
    When the ETL pipeline processes web sales data
    Then the output should contain records with channel = 'web'
    And each record should have valid joins between web_sales, date_dim, and web_site tables
    And sales_date should match d_date from date_dim table
    And year should match d_year from date_dim table
    And month should match d_moy from date_dim table
    And quarter should match d_qoy from date_dim table
    And day_name should match d_day_name from date_dim table
    And channel_location should match web_name from web_site table
    And channel_state should match web_state from web_site table
    And total_quantity should equal sum of ws_quantity grouped by date and web_site
    And total_sales_amount should equal sum of ws_ext_sales_price grouped by date and web_site
    And total_net_paid should equal sum of ws_net_paid grouped by date and web_site
    And total_net_profit should equal sum of ws_net_profit grouped by date and web_site
    And avg_sales_price should equal total_sales_amount divided by total_quantity

Scenario: Test catalog sales data transformation and aggregation
    Given the source tables are populated with test data
    When the ETL pipeline processes catalog sales data
    Then the output should contain records with channel = 'catalog'
    And each record should have valid joins between catalog_sales, date_dim, and call_center tables
    And sales_date should match d_date from date_dim table
    And year should match d_year from date_dim table
    And month should match d_moy from date_dim table
    And quarter should match d_qoy from date_dim table
    And day_name should match d_day_name from date_dim table
    And channel_location should match cc_name from call_center table
    And channel_state should match cc_state from call_center table
    And total_quantity should equal sum of cs_quantity grouped by date and call_center
    And total_sales_amount should equal sum of cs_ext_sales_price grouped by date and call_center
    And total_net_paid should equal sum of cs_net_paid grouped by date and call_center
    And total_net_profit should equal sum of cs_net_profit grouped by date and call_center
    And avg_sales_price should equal total_sales_amount divided by total_quantity

Scenario: Test data consolidation across all channels
    Given all individual channel transformations are completed
    When the ETL pipeline consolidates all channel data
    Then the final output should contain records from all three channels: 'store', 'web', 'catalog'
    And records should be properly grouped by sales_date, year, month, quarter, day_name, channel, channel_location, and channel_state
    And total_quantity should be the sum of quantities for each group
    And total_sales_amount should be the sum of sales amounts for each group
    And total_net_paid should be the sum of net paid amounts for each group
    And total_net_profit should be the sum of net profits for each group
    And transaction_count should be the count of transactions for each group
    And avg_sales_price should be recalculated as total_sales_amount divided by total_quantity for each group

Scenario: Test data quality and integrity
    Given the ETL pipeline has completed processing
    Then all records in sales_summary_by_channel should have non-null values for required fields
    And sales_date should be a valid date
    And year should be between 2014 and current year
    And month should be between 1 and 12
    And quarter should be between 1 and 4
    And channel should be one of: 'store', 'web', 'catalog'
    And all numeric fields should be non-negative
    And avg_sales_price should equal total_sales_amount divided by total_quantity with precision tolerance of 0.01

Scenario: Test edge cases with null and zero values
    Given source tables contain records with null values in optional fields
    And source tables contain records with zero quantities and amounts
    When the ETL pipeline processes the data
    Then records with null join keys should be excluded from output
    And records with zero quantities should be included but avg_sales_price should handle division by zero
    And aggregated totals should correctly handle null values by treating them as zero

Scenario: Test performance with large data volumes
    Given source tables are populated with 100000 store_sales records
    And source tables are populated with 80000 web_sales records  
    And source tables are populated with 60000 catalog_sales records
    When the ETL pipeline processes all data
    Then the pipeline should complete within acceptable time limits
    And memory usage should remain within defined thresholds
    And the output record count should match expected aggregation results

Scenario: Test incremental data processing
    Given initial data load has been processed
    And new incremental data is added to source tables
    When the ETL pipeline processes incremental data
    Then only new and changed records should be processed
    And existing aggregations should be updated correctly
    And final output should reflect both historical and new data accurately