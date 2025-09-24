Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background:
    Given the following source table schemas:
      | Table Name  | Column Name         | Data Type |
      | store_sales | ss_sold_date_sk     | NUMBER    |
      | store_sales | ss_item_sk          | NUMBER    |
      | store_sales | ss_store_sk         | NUMBER    |
      | store_sales | ss_quantity         | NUMBER    |
      | store_sales | ss_sales_price      | NUMBER    |
      | store_sales | ss_ext_sales_price  | NUMBER    |
      | store_sales | ss_net_paid         | NUMBER    |
      | item        | i_item_sk           | NUMBER    |
      | item        | i_item_id           | STRING    |
      | item        | i_item_desc         | STRING    |
      | item        | i_brand             | STRING    |
      | item        | i_category          | STRING    |
      | item        | i_product_name      | STRING    |
      | store       | s_store_sk          | NUMBER    |
      | store       | s_store_id          | STRING    |
      | store       | s_store_name        | STRING    |
      | store       | s_city              | STRING    |
      | store       | s_state             | STRING    |
      | date_dim    | d_date_sk           | NUMBER    |
      | date_dim    | d_date              | DATE      |
      | date_dim    | d_year              | NUMBER    |
      | date_dim    | d_moy               | NUMBER    |
      | date_dim    | d_month_seq         | NUMBER    |

    And the following target table schema:
      | Table Name         | Column Name        | Data Type |
      | fact_monthly_sales | date_key           | NUMBER    |
      | fact_monthly_sales | item_key           | NUMBER    |
      | fact_monthly_sales | store_key          | NUMBER    |
      | fact_monthly_sales | year_month         | STRING    |
      | fact_monthly_sales | total_quantity     | NUMBER    |
      | fact_monthly_sales | total_sales_amount | NUMBER    |
      | fact_monthly_sales | total_net_paid     | NUMBER    |
      | fact_monthly_sales | transaction_count  | NUMBER    |
      | dim_item           | item_key           | NUMBER    |
      | dim_item           | item_id            | STRING    |
      | dim_item           | item_description   | STRING    |
      | dim_item           | brand_name         | STRING    |
      | dim_item           | category_name      | STRING    |
      | dim_item           | product_name       | STRING    |
      | dim_store          | store_key          | NUMBER    |
      | dim_store          | store_id           | STRING    |
      | dim_store          | store_name         | STRING    |
      | dim_store          | city               | STRING    |
      | dim_store          | state              | STRING    |
      | dim_date           | date_key           | NUMBER    |
      | dim_date           | calendar_date      | DATE      |
      | dim_date           | year_number        | NUMBER    |
      | dim_date           | month_number       | NUMBER    |
      | dim_date           | month_sequence     | NUMBER    |

  Scenario: Extract and load item dimension
    Given source data from item table
    When transforming item data
    Then map item_key from i_item_sk
    And map item_id from i_item_id
    And map item_description from i_item_desc
    And map brand_name from i_brand
    And map category_name from i_category
    And map product_name from i_product_name
    And load data into dim_item table

  Scenario: Extract and load store dimension
    Given source data from store table
    When transforming store data
    Then map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state
    And load data into dim_store table

  Scenario: Extract and load date dimension
    Given source data from date_dim table
    When transforming date data
    Then map date_key from d_date_sk
    And map calendar_date from d_date
    And map year_number from d_year
    And map month_number from d_moy
    And map month_sequence from d_month_seq
    And load data into dim_date table

  Scenario: Extract and load monthly sales fact table
    Given source data from store_sales table
    And join with date_dim table on ss_sold_date_sk equals d_date_sk
    And join with item table on ss_item_sk equals i_item_sk
    And join with store table on ss_store_sk equals s_store_sk
    When aggregating sales data by month, item, and store
    Then map date_key from d_date_sk
    And map item_key from i_item_sk
    And map store_key from s_store_sk
    And map year_month by concatenating d_year and d_moy with hyphen separator
    And map total_quantity by summing ss_quantity
    And map total_sales_amount by summing ss_ext_sales_price
    And map total_net_paid by summing ss_net_paid
    And map transaction_count by counting distinct ss_ticket_number
    And group by d_date_sk, i_item_sk, s_store_sk, d_year, d_moy
    And load aggregated data into fact_monthly_sales table

  Scenario: Support sales by item per month analysis
    Given fact_monthly_sales table is loaded
    When querying for sales by item and month
    Then join fact_monthly_sales with dim_item on item_key
    And join fact_monthly_sales with dim_date on date_key
    And aggregate total_sales_amount by item and year_month

  Scenario: Support sales by store per month analysis
    Given fact_monthly_sales table is loaded
    When querying for sales by store and month
    Then join fact_monthly_sales with dim_store on store_key
    And join fact_monthly_sales with dim_date on date_key
    And aggregate total_sales_amount by store and year_month