Feature: monthly_sales_star_schema
  Create a star schema to analyze monthly sales performance with KPIs for sales by item/month and sales by store/month

  Background: Source Schema
    Given source table "store_sales" with schema:
      | column_name           | data_type |
      | ss_sold_date_sk       | NUMBER    |
      | ss_item_sk            | NUMBER    |
      | ss_store_sk           | NUMBER    |
      | ss_quantity           | NUMBER    |
      | ss_sales_price        | NUMBER    |
      | ss_ext_sales_price    | NUMBER    |
      | ss_net_paid           | NUMBER    |
      | ss_net_profit         | NUMBER    |
    
    And source table "date_dim" with schema:
      | column_name  | data_type |
      | d_date_sk    | NUMBER    |
      | d_date       | DATE      |
      | d_year       | NUMBER    |
      | d_moy        | NUMBER    |
      | d_month_seq  | NUMBER    |
    
    And source table "item" with schema:
      | column_name     | data_type |
      | i_item_sk       | NUMBER    |
      | i_item_id       | STRING    |
      | i_item_desc     | STRING    |
      | i_brand         | STRING    |
      | i_category      | STRING    |
      | i_class         | STRING    |
      | i_product_name  | STRING    |
    
    And source table "store" with schema:
      | column_name     | data_type |
      | s_store_sk      | NUMBER    |
      | s_store_id      | STRING    |
      | s_store_name    | STRING    |
      | s_market_desc   | STRING    |
      | s_division_name | STRING    |
      | s_company_name  | STRING    |
      | s_city          | STRING    |
      | s_state         | STRING    |

  Scenario: Create fact_monthly_sales table
    Given target table "fact_monthly_sales" with schema:
      | column_name        | data_type |
      | date_key          | NUMBER    |
      | item_key          | NUMBER    |
      | store_key         | NUMBER    |
      | year              | NUMBER    |
      | month             | NUMBER    |
      | month_seq         | NUMBER    |
      | total_quantity    | NUMBER    |
      | total_sales       | NUMBER    |
      | total_net_paid    | NUMBER    |
      | total_net_profit  | NUMBER    |
      | transaction_count | NUMBER    |
    
    When joining "store_sales" to "date_dim" on ss_sold_date_sk equals d_date_sk
    Then map d_date_sk to date_key
    And map ss_item_sk to item_key  
    And map ss_store_sk to store_key
    And map d_year to year
    And map d_moy to month
    And map d_month_seq to month_seq
    And sum ss_quantity grouped by date_key, item_key, store_key, year, month, month_seq to total_quantity
    And sum ss_ext_sales_price grouped by date_key, item_key, store_key, year, month, month_seq to total_sales
    And sum ss_net_paid grouped by date_key, item_key, store_key, year, month, month_seq to total_net_paid
    And sum ss_net_profit grouped by date_key, item_key, store_key, year, month, month_seq to total_net_profit
    And count distinct ss_ticket_number grouped by date_key, item_key, store_key, year, month, month_seq to transaction_count

  Scenario: Create dim_item table
    Given target table "dim_item" with schema:
      | column_name  | data_type |
      | item_key     | NUMBER    |
      | item_id      | STRING    |
      | item_name    | STRING    |
      | item_desc    | STRING    |
      | brand        | STRING    |
      | category     | STRING    |
      | class        | STRING    |
    
    When extracting from "item"
    Then map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_product_name to item_name
    And map i_item_desc to item_desc
    And map i_brand to brand
    And map i_category to category
    And map i_class to class

  Scenario: Create dim_store table
    Given target table "dim_store" with schema:
      | column_name   | data_type |
      | store_key     | NUMBER    |
      | store_id      | STRING    |
      | store_name    | STRING    |
      | market_desc   | STRING    |
      | division_name | STRING    |
      | company_name  | STRING    |
      | city          | STRING    |
      | state         | STRING    |
    
    When extracting from "store"
    Then map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_market_desc to market_desc
    And map s_division_name to division_name
    And map s_company_name to company_name
    And map s_city to city
    And map s_state to state

  Scenario: Create dim_date table
    Given target table "dim_date" with schema:
      | column_name | data_type |
      | date_key    | NUMBER    |
      | date        | DATE      |
      | year        | NUMBER    |
      | month       | NUMBER    |
      | month_seq   | NUMBER    |
    
    When extracting from "date_dim"
    Then map d_date_sk to date_key
    And map d_date to date
    And map d_year to year
    And map d_moy to month
    And map d_month_seq to month_seq