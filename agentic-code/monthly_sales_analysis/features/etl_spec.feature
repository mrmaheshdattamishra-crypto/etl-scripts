Feature: monthly_sales_analysis
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store over time

  Background: Source Tables Schema
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
    
    And source table "item" with columns:
      | column_name    | data_type |
      | i_item_sk      | NUMBER    |
      | i_item_id      | STRING    |
      | i_item_desc    | STRING    |
      | i_brand        | STRING    |
      | i_class        | STRING    |
      | i_category     | STRING    |
      | i_product_name | STRING    |
    
    And source table "store" with columns:
      | column_name      | data_type |
      | s_store_sk       | NUMBER    |
      | s_store_id       | STRING    |
      | s_store_name     | STRING    |
      | s_city           | STRING    |
      | s_state          | STRING    |
      | s_division_name  | STRING    |
      | s_company_name   | STRING    |
    
    And source table "date_dim" with columns:
      | column_name | data_type |
      | d_date_sk   | NUMBER    |
      | d_date      | DATE      |
      | d_year      | NUMBER    |
      | d_moy       | NUMBER    |
      | d_month_seq | NUMBER    |

  Scenario: Create fact table for monthly sales
    Given target table "fact_monthly_sales" with columns:
      | column_name          | data_type |
      | date_key            | NUMBER    |
      | item_key            | NUMBER    |
      | store_key           | NUMBER    |
      | year                | NUMBER    |
      | month               | NUMBER    |
      | month_sequence      | NUMBER    |
      | total_quantity      | NUMBER    |
      | total_sales_amount  | NUMBER    |
      | total_net_paid      | NUMBER    |
      | total_net_profit    | NUMBER    |
      | transaction_count   | NUMBER    |
    
    When I join "store_sales" with "date_dim" on store_sales.ss_sold_date_sk equals date_dim.d_date_sk
    Then I map date_key as d_date_sk
    And I map item_key as ss_item_sk
    And I map store_key as ss_store_sk
    And I map year as d_year
    And I map month as d_moy
    And I map month_sequence as d_month_seq
    And I map total_quantity as sum of ss_quantity grouped by year, month, item_key, store_key
    And I map total_sales_amount as sum of ss_ext_sales_price grouped by year, month, item_key, store_key
    And I map total_net_paid as sum of ss_net_paid grouped by year, month, item_key, store_key
    And I map total_net_profit as sum of ss_net_profit grouped by year, month, item_key, store_key
    And I map transaction_count as count of distinct ss_ticket_number grouped by year, month, item_key, store_key

  Scenario: Create item dimension table
    Given target table "dim_item" with columns:
      | column_name    | data_type |
      | item_key       | NUMBER    |
      | item_id        | STRING    |
      | item_name      | STRING    |
      | item_description | STRING  |
      | brand_name     | STRING    |
      | class_name     | STRING    |
      | category_name  | STRING    |
    
    When I select from "item"
    Then I map item_key as i_item_sk
    And I map item_id as i_item_id
    And I map item_name as i_product_name
    And I map item_description as i_item_desc
    And I map brand_name as i_brand
    And I map class_name as i_class
    And I map category_name as i_category

  Scenario: Create store dimension table
    Given target table "dim_store" with columns:
      | column_name      | data_type |
      | store_key        | NUMBER    |
      | store_id         | STRING    |
      | store_name       | STRING    |
      | city             | STRING    |
      | state            | STRING    |
      | division_name    | STRING    |
      | company_name     | STRING    |
    
    When I select from "store"
    Then I map store_key as s_store_sk
    And I map store_id as s_store_id
    And I map store_name as s_store_name
    And I map city as s_city
    And I map state as s_state
    And I map division_name as s_division_name
    And I map company_name as s_company_name

  Scenario: Create date dimension table
    Given target table "dim_date" with columns:
      | column_name     | data_type |
      | date_key        | NUMBER    |
      | calendar_date   | DATE      |
      | year            | NUMBER    |
      | month           | NUMBER    |
      | month_sequence  | NUMBER    |
    
    When I select from "date_dim"
    Then I map date_key as d_date_sk
    And I map calendar_date as d_date
    And I map year as d_year
    And I map month as d_moy
    And I map month_sequence as d_month_seq

  Scenario: Create aggregated view for sales by item and month
    Given target view "sales_by_item_month" with columns:
      | column_name         | data_type |
      | year                | NUMBER    |
      | month               | NUMBER    |
      | item_name           | STRING    |
      | brand_name          | STRING    |
      | category_name       | STRING    |
      | total_sales_amount  | NUMBER    |
      | total_quantity      | NUMBER    |
      | total_net_profit    | NUMBER    |
    
    When I join "fact_monthly_sales" with "dim_item" on fact_monthly_sales.item_key equals dim_item.item_key
    Then I map year as fact_monthly_sales.year
    And I map month as fact_monthly_sales.month
    And I map item_name as dim_item.item_name
    And I map brand_name as dim_item.brand_name
    And I map category_name as dim_item.category_name
    And I map total_sales_amount as sum of fact_monthly_sales.total_sales_amount grouped by year, month, item_name, brand_name, category_name
    And I map total_quantity as sum of fact_monthly_sales.total_quantity grouped by year, month, item_name, brand_name, category_name
    And I map total_net_profit as sum of fact_monthly_sales.total_net_profit grouped by year, month, item_name, brand_name, category_name

  Scenario: Create aggregated view for sales by store and month
    Given target view "sales_by_store_month" with columns:
      | column_name         | data_type |
      | year                | NUMBER    |
      | month               | NUMBER    |
      | store_name          | STRING    |
      | city                | STRING    |
      | state               | STRING    |
      | division_name       | STRING    |
      | total_sales_amount  | NUMBER    |
      | total_quantity      | NUMBER    |
      | total_net_profit    | NUMBER    |
    
    When I join "fact_monthly_sales" with "dim_store" on fact_monthly_sales.store_key equals dim_store.store_key
    Then I map year as fact_monthly_sales.year
    And I map month as fact_monthly_sales.month
    And I map store_name as dim_store.store_name
    And I map city as dim_store.city
    And I map state as dim_store.state
    And I map division_name as dim_store.division_name
    And I map total_sales_amount as sum of fact_monthly_sales.total_sales_amount grouped by year, month, store_name, city, state, division_name
    And I map total_quantity as sum of fact_monthly_sales.total_quantity grouped by year, month, store_name, city, state, division_name
    And I map total_net_profit as sum of fact_monthly_sales.total_net_profit grouped by year, month, store_name, city, state, division_name