Feature: monthly_sales_analytics
  As a data analyst
  I want to create a star schema for monthly sales analysis
  So that I can analyze sales by item/month and sales by store/month

  Background:
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk: NUMBER, ss_item_sk: NUMBER, ss_store_sk: NUMBER, ss_quantity: NUMBER, ss_sales_price: NUMBER, ss_ext_sales_price: NUMBER, ss_net_paid: NUMBER, ss_net_profit: NUMBER |
      | item        | i_item_sk: NUMBER, i_item_id: STRING, i_item_desc: STRING, i_brand: STRING, i_class: STRING, i_category: STRING, i_product_name: STRING |
      | store       | s_store_sk: NUMBER, s_store_id: STRING, s_store_name: STRING, s_city: STRING, s_state: STRING, s_country: STRING |
      | date_dim    | d_date_sk: NUMBER, d_date: DATE, d_year: NUMBER, d_moy: NUMBER, d_month_seq: NUMBER |

  Scenario: Create fact table for monthly sales
    Given I need to create a fact_monthly_sales table
    When I process the source data
    Then the target table should have the following schema:
      | table_name          | columns |
      | fact_monthly_sales  | date_sk: NUMBER, item_sk: NUMBER, store_sk: NUMBER, year: NUMBER, month: NUMBER, month_seq: NUMBER, total_quantity: NUMBER, total_sales_amount: NUMBER, total_net_paid: NUMBER, total_net_profit: NUMBER, transaction_count: NUMBER |
    And the data should be mapped as follows:
      | source_field                    | target_field          | transformation |
      | date_dim.d_date_sk             | date_sk               | direct mapping from date dimension |
      | item.i_item_sk                 | item_sk               | direct mapping from item dimension |
      | store.s_store_sk               | store_sk              | direct mapping from store dimension |
      | date_dim.d_year                | year                  | extract year from date dimension |
      | date_dim.d_moy                 | month                 | extract month number from date dimension |
      | date_dim.d_month_seq           | month_seq             | extract month sequence from date dimension |
      | store_sales.ss_quantity        | total_quantity        | sum of all quantities for the month |
      | store_sales.ss_ext_sales_price | total_sales_amount    | sum of extended sales price for the month |
      | store_sales.ss_net_paid        | total_net_paid        | sum of net paid amount for the month |
      | store_sales.ss_net_profit      | total_net_profit      | sum of net profit for the month |
      | store_sales records            | transaction_count     | count of sales transactions for the month |
    And the join relationships should be:
      | left_table   | left_key        | right_table | right_key   | join_type |
      | store_sales  | ss_sold_date_sk | date_dim    | d_date_sk   | inner     |
      | store_sales  | ss_item_sk      | item        | i_item_sk   | inner     |
      | store_sales  | ss_store_sk     | store       | s_store_sk  | inner     |
    And the aggregation should group by year, month, item_sk, and store_sk

  Scenario: Create item dimension table
    Given I need to create a dim_item table
    When I process the item source data
    Then the target table should have the following schema:
      | table_name | columns |
      | dim_item   | item_sk: NUMBER, item_id: STRING, item_description: STRING, brand: STRING, class: STRING, category: STRING, product_name: STRING |
    And the data should be mapped as follows:
      | source_field      | target_field       | transformation |
      | item.i_item_sk    | item_sk           | direct mapping |
      | item.i_item_id    | item_id           | direct mapping |
      | item.i_item_desc  | item_description  | direct mapping |
      | item.i_brand      | brand             | direct mapping |
      | item.i_class      | class             | direct mapping |
      | item.i_category   | category          | direct mapping |
      | item.i_product_name | product_name    | direct mapping |

  Scenario: Create store dimension table
    Given I need to create a dim_store table
    When I process the store source data
    Then the target table should have the following schema:
      | table_name | columns |
      | dim_store  | store_sk: NUMBER, store_id: STRING, store_name: STRING, city: STRING, state: STRING, country: STRING |
    And the data should be mapped as follows:
      | source_field     | target_field | transformation |
      | store.s_store_sk | store_sk     | direct mapping |
      | store.s_store_id | store_id     | direct mapping |
      | store.s_store_name | store_name | direct mapping |
      | store.s_city     | city         | direct mapping |
      | store.s_state    | state        | direct mapping |
      | store.s_country  | country      | direct mapping |

  Scenario: Create date dimension table
    Given I need to create a dim_date table
    When I process the date_dim source data
    Then the target table should have the following schema:
      | table_name | columns |
      | dim_date   | date_sk: NUMBER, date_value: DATE, year: NUMBER, month: NUMBER, month_seq: NUMBER |
    And the data should be mapped as follows:
      | source_field        | target_field | transformation |
      | date_dim.d_date_sk  | date_sk      | direct mapping |
      | date_dim.d_date     | date_value   | direct mapping |
      | date_dim.d_year     | year         | direct mapping |
      | date_dim.d_moy      | month        | direct mapping |
      | date_dim.d_month_seq | month_seq   | direct mapping |

  Scenario: Enable KPI analysis for sales by item per month
    Given the star schema is created with fact_monthly_sales and dimension tables
    When querying sales by item per month
    Then I can join fact_monthly_sales with dim_item and dim_date
    And aggregate total_sales_amount, total_quantity, and transaction_count by item and month
    And filter by specific time periods using year and month fields

  Scenario: Enable KPI analysis for sales by store per month  
    Given the star schema is created with fact_monthly_sales and dimension tables
    When querying sales by store per month
    Then I can join fact_monthly_sales with dim_store and dim_date
    And aggregate total_sales_amount, total_quantity, and transaction_count by store and month
    And filter by specific time periods using year and month fields