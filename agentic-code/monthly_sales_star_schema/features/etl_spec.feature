# Referenced similar ticket SCRUM-9 for monthly sales analysis patterns

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track KPIs for sales by item/month and sales by store/month

  Background: Source Tables Schema
    Given source table "store_sales" with schema:
      | column_name           | data_type      |
      | ss_sold_date_sk       | NUMBER         |
      | ss_item_sk            | NUMBER         |
      | ss_store_sk           | NUMBER         |
      | ss_quantity           | NUMBER         |
      | ss_sales_price        | NUMBER         |
      | ss_ext_sales_price    | NUMBER         |
      | ss_net_paid           | NUMBER         |
      | ss_net_profit         | NUMBER         |
    
    And source table "item" with schema:
      | column_name     | data_type |
      | i_item_sk       | NUMBER    |
      | i_item_id       | STRING    |
      | i_item_desc     | STRING    |
      | i_brand         | STRING    |
      | i_category      | STRING    |
      | i_product_name  | STRING    |
    
    And source table "store" with schema:
      | column_name       | data_type |
      | s_store_sk        | NUMBER    |
      | s_store_id        | STRING    |
      | s_store_name      | STRING    |
      | s_city            | STRING    |
      | s_state           | STRING    |
      | s_division_name   | STRING    |
      | s_company_name    | STRING    |
    
    And source table "date_dim" with schema:
      | column_name | data_type |
      | d_date_sk   | NUMBER    |
      | d_date      | DATE      |
      | d_year      | NUMBER    |
      | d_moy       | NUMBER    |
      | d_dom       | NUMBER    |

  Scenario: Create fact table for monthly sales
    Given target table "fact_monthly_sales" with schema:
      | column_name          | data_type |
      | date_key             | NUMBER    |
      | item_key             | NUMBER    |
      | store_key            | NUMBER    |
      | year                 | NUMBER    |
      | month                | NUMBER    |
      | total_quantity       | NUMBER    |
      | total_sales_amount   | NUMBER    |
      | total_net_paid       | NUMBER    |
      | total_net_profit     | NUMBER    |
      | transaction_count    | NUMBER    |
    
    When I join "store_sales" to "date_dim" on ss_sold_date_sk equals d_date_sk
    Then I aggregate sales data by date_key, item_key, store_key, year, and month
    And I calculate total_quantity as sum of ss_quantity
    And I calculate total_sales_amount as sum of ss_ext_sales_price
    And I calculate total_net_paid as sum of ss_net_paid
    And I calculate total_net_profit as sum of ss_net_profit
    And I calculate transaction_count as count of distinct transactions
    And I use ss_sold_date_sk as date_key
    And I use ss_item_sk as item_key
    And I use ss_store_sk as store_key
    And I use d_year as year
    And I use d_moy as month

  Scenario: Create item dimension table
    Given target table "dim_item" with schema:
      | column_name    | data_type |
      | item_key       | NUMBER    |
      | item_id        | STRING    |
      | item_name      | STRING    |
      | item_desc      | STRING    |
      | brand          | STRING    |
      | category       | STRING    |
    
    When I select from "item" table
    Then I map i_item_sk to item_key
    And I map i_item_id to item_id
    And I map i_product_name to item_name
    And I map i_item_desc to item_desc
    And I map i_brand to brand
    And I map i_category to category

  Scenario: Create store dimension table
    Given target table "dim_store" with schema:
      | column_name   | data_type |
      | store_key     | NUMBER    |
      | store_id      | STRING    |
      | store_name    | STRING    |
      | city          | STRING    |
      | state         | STRING    |
      | division      | STRING    |
      | company       | STRING    |
    
    When I select from "store" table
    Then I map s_store_sk to store_key
    And I map s_store_id to store_id
    And I map s_store_name to store_name
    And I map s_city to city
    And I map s_state to state
    And I map s_division_name to division
    And I map s_company_name to company

  Scenario: Create date dimension table
    Given target table "dim_date" with schema:
      | column_name | data_type |
      | date_key    | NUMBER    |
      | date_value  | DATE      |
      | year        | NUMBER    |
      | month       | NUMBER    |
      | day         | NUMBER    |
    
    When I select from "date_dim" table
    Then I map d_date_sk to date_key
    And I map d_date to date_value
    And I map d_year to year
    And I map d_moy to month
    And I map d_dom to day