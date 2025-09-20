Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store over time

  Background: Source Schema
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_paid (NUMBER), ss_net_profit (NUMBER) |
      | item        | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_class (STRING), i_category (STRING), i_product_name (STRING) |
      | store       | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_country (STRING) |
      | date_dim    | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_month_seq (NUMBER) |

  Scenario: Create fact table for monthly sales
    Given I want to create a fact table called "fact_monthly_sales"
    When I aggregate sales data by month, item, and store
    Then the target table should have the following schema:
      | column_name           | data_type | description |
      | date_key             | NUMBER    | Foreign key to date dimension |
      | item_key             | NUMBER    | Foreign key to item dimension |
      | store_key            | NUMBER    | Foreign key to store dimension |
      | sales_month_key      | NUMBER    | Year-month identifier for aggregation |
      | total_quantity       | NUMBER    | Sum of quantities sold |
      | total_sales_amount   | NUMBER    | Sum of extended sales price |
      | total_net_paid       | NUMBER    | Sum of net paid amount |
      | total_net_profit     | NUMBER    | Sum of net profit |
      | transaction_count    | NUMBER    | Count of individual transactions |
    And the data should be joined as follows:
      | left_table   | left_column     | right_table | right_column |
      | store_sales  | ss_sold_date_sk | date_dim    | d_date_sk    |
      | store_sales  | ss_item_sk      | item        | i_item_sk    |
      | store_sales  | ss_store_sk     | store       | s_store_sk   |
    And the mapping logic should be:
      - Extract year and month from date dimension to create monthly grouping key
      - Aggregate all sales metrics by year-month, item, and store combination
      - Calculate total quantity as sum of ss_quantity for each group
      - Calculate total sales amount as sum of ss_ext_sales_price for each group
      - Calculate total net paid as sum of ss_net_paid for each group
      - Calculate total net profit as sum of ss_net_profit for each group
      - Count number of distinct transactions for each group

  Scenario: Create item dimension table
    Given I want to create a dimension table called "dim_item"
    When I extract item master data
    Then the target table should have the following schema:
      | column_name    | data_type | description |
      | item_key       | NUMBER    | Primary key |
      | item_id        | STRING    | Business item identifier |
      | item_name      | STRING    | Product name |
      | item_desc      | STRING    | Item description |
      | brand_name     | STRING    | Brand name |
      | item_class     | STRING    | Item class |
      | item_category  | STRING    | Item category |
    And the mapping logic should be:
      - Use i_item_sk as item_key
      - Use i_item_id as item_id
      - Use i_product_name as item_name
      - Use i_item_desc as item_desc
      - Use i_brand as brand_name
      - Use i_class as item_class
      - Use i_category as item_category

  Scenario: Create store dimension table
    Given I want to create a dimension table called "dim_store"
    When I extract store master data
    Then the target table should have the following schema:
      | column_name   | data_type | description |
      | store_key     | NUMBER    | Primary key |
      | store_id      | STRING    | Business store identifier |
      | store_name    | STRING    | Store name |
      | store_city    | STRING    | Store city |
      | store_state   | STRING    | Store state |
      | store_country | STRING    | Store country |
    And the mapping logic should be:
      - Use s_store_sk as store_key
      - Use s_store_id as store_id
      - Use s_store_name as store_name
      - Use s_city as store_city
      - Use s_state as store_state
      - Use s_country as store_country

  Scenario: Create date dimension table
    Given I want to create a dimension table called "dim_date"
    When I extract date master data
    Then the target table should have the following schema:
      | column_name    | data_type | description |
      | date_key       | NUMBER    | Primary key |
      | calendar_date  | DATE      | Calendar date |
      | year_number    | NUMBER    | Year |
      | month_number   | NUMBER    | Month number |
      | month_key      | NUMBER    | Year-month key for aggregation |
    And the mapping logic should be:
      - Use d_date_sk as date_key
      - Use d_date as calendar_date
      - Use d_year as year_number
      - Use d_moy as month_number
      - Create month_key by combining year and month as YYYYMM format