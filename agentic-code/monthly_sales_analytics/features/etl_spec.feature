Feature: monthly_sales_analytics
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background: Source Schema
    Given source table "store_sales" with columns:
      | column_name           | data_type |
      | ss_sold_date_sk       | NUMBER    |
      | ss_item_sk            | NUMBER    |
      | ss_store_sk           | NUMBER    |
      | ss_quantity           | NUMBER    |
      | ss_sales_price        | NUMBER    |
      | ss_ext_sales_price    | NUMBER    |
      | ss_net_paid           | NUMBER    |
      | ss_net_profit         | NUMBER    |
    And source table "date_dim" with columns:
      | column_name    | data_type |
      | d_date_sk      | NUMBER    |
      | d_date         | DATE      |
      | d_year         | NUMBER    |
      | d_moy          | NUMBER    |
      | d_month_seq    | NUMBER    |
    And source table "item" with columns:
      | column_name    | data_type |
      | i_item_sk      | NUMBER    |
      | i_item_id      | STRING    |
      | i_item_desc    | STRING    |
      | i_brand        | STRING    |
      | i_category     | STRING    |
      | i_product_name | STRING    |
    And source table "store" with columns:
      | column_name   | data_type |
      | s_store_sk    | NUMBER    |
      | s_store_id    | STRING    |
      | s_store_name  | STRING    |
      | s_city        | STRING    |
      | s_state       | STRING    |

  Scenario: Create fact table for monthly sales
    Given target table "fact_monthly_sales" with columns:
      | column_name        | data_type |
      | date_key           | NUMBER    |
      | item_key           | NUMBER    |
      | store_key          | NUMBER    |
      | sales_month        | NUMBER    |
      | sales_year         | NUMBER    |
      | total_quantity     | NUMBER    |
      | total_sales_amount | NUMBER    |
      | total_net_paid     | NUMBER    |
      | total_profit       | NUMBER    |
    When joining "store_sales" to "date_dim" on ss_sold_date_sk equals d_date_sk
    And joining "store_sales" to "item" on ss_item_sk equals i_item_sk
    And joining "store_sales" to "store" on ss_store_sk equals s_store_sk
    Then map date_key from d_date_sk
    And map item_key from i_item_sk
    And map store_key from s_store_sk
    And map sales_month from d_moy
    And map sales_year from d_year
    And map total_quantity by summing ss_quantity grouped by year, month, item, and store
    And map total_sales_amount by summing ss_ext_sales_price grouped by year, month, item, and store
    And map total_net_paid by summing ss_net_paid grouped by year, month, item, and store
    And map total_profit by summing ss_net_profit grouped by year, month, item, and store

  Scenario: Create dimension table for items
    Given target table "dim_item" with columns:
      | column_name  | data_type |
      | item_key     | NUMBER    |
      | item_id      | STRING    |
      | item_name    | STRING    |
      | item_desc    | STRING    |
      | brand        | STRING    |
      | category     | STRING    |
    When selecting from "item"
    Then map item_key from i_item_sk
    And map item_id from i_item_id
    And map item_name from i_product_name
    And map item_desc from i_item_desc
    And map brand from i_brand
    And map category from i_category

  Scenario: Create dimension table for stores
    Given target table "dim_store" with columns:
      | column_name | data_type |
      | store_key   | NUMBER    |
      | store_id    | STRING    |
      | store_name  | STRING    |
      | city        | STRING    |
      | state       | STRING    |
    When selecting from "store"
    Then map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state

  Scenario: Create dimension table for dates
    Given target table "dim_date" with columns:
      | column_name | data_type |
      | date_key    | NUMBER    |
      | date_value  | DATE      |
      | year        | NUMBER    |
      | month       | NUMBER    |
      | month_seq   | NUMBER    |
    When selecting from "date_dim"
    Then map date_key from d_date_sk
    And map date_value from d_date
    And map year from d_year
    And map month from d_moy
    And map month_seq from d_month_seq