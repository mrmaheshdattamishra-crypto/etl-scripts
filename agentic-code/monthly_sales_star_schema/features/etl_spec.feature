Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store over time

  Background: Source Tables Schema
    Given source table "store_sales" with columns:
      | column_name           | data_type |
      | ss_sold_date_sk      | NUMBER    |
      | ss_item_sk           | NUMBER    |
      | ss_store_sk          | NUMBER    |
      | ss_quantity          | NUMBER    |
      | ss_sales_price       | NUMBER    |
      | ss_ext_sales_price   | NUMBER    |
      | ss_net_paid          | NUMBER    |
      | ss_net_profit        | NUMBER    |
    
    And source table "date_dim" with columns:
      | column_name | data_type |
      | d_date_sk   | NUMBER    |
      | d_date      | DATE      |
      | d_year      | NUMBER    |
      | d_moy       | NUMBER    |
      | d_month_seq | NUMBER    |
    
    And source table "item" with columns:
      | column_name   | data_type |
      | i_item_sk     | NUMBER    |
      | i_item_id     | STRING    |
      | i_item_desc   | STRING    |
      | i_brand       | STRING    |
      | i_category    | STRING    |
      | i_class       | STRING    |
      | i_product_name| STRING    |
    
    And source table "store" with columns:
      | column_name     | data_type |
      | s_store_sk      | NUMBER    |
      | s_store_id      | STRING    |
      | s_store_name    | STRING    |
      | s_city          | STRING    |
      | s_state         | STRING    |
      | s_market_desc   | STRING    |
      | s_division_name | STRING    |

  Scenario: Create fact table for monthly sales
    Given target table "fact_monthly_sales" with columns:
      | column_name        | data_type |
      | date_key           | NUMBER    |
      | item_key           | NUMBER    |
      | store_key          | NUMBER    |
      | sales_year         | NUMBER    |
      | sales_month        | NUMBER    |
      | total_quantity     | NUMBER    |
      | total_sales_amount | NUMBER    |
      | total_net_paid     | NUMBER    |
      | total_net_profit   | NUMBER    |
      | transaction_count  | NUMBER    |
    
    When transforming from source to target
    Then join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And group data by d_year, d_moy, ss_item_sk, and ss_store_sk
    And map d_date_sk to date_key using the date surrogate key
    And map ss_item_sk to item_key using the item surrogate key
    And map ss_store_sk to store_key using the store surrogate key
    And map d_year to sales_year using the year from date dimension
    And map d_moy to sales_month using the month of year from date dimension
    And map sum of ss_quantity to total_quantity aggregating all quantities sold
    And map sum of ss_ext_sales_price to total_sales_amount aggregating all extended sales prices
    And map sum of ss_net_paid to total_net_paid aggregating all net payments
    And map sum of ss_net_profit to total_net_profit aggregating all net profits
    And map count of distinct ss_ticket_number to transaction_count counting unique transactions

  Scenario: Create dimension table for items
    Given target table "dim_item" with columns:
      | column_name    | data_type |
      | item_key       | NUMBER    |
      | item_id        | STRING    |
      | item_desc      | STRING    |
      | brand_name     | STRING    |
      | category_name  | STRING    |
      | class_name     | STRING    |
      | product_name   | STRING    |
    
    When transforming from source to target
    Then map i_item_sk to item_key using the item surrogate key
    And map i_item_id to item_id using the item identifier
    And map i_item_desc to item_desc using the item description
    And map i_brand to brand_name using the brand information
    And map i_category to category_name using the category information
    And map i_class to class_name using the class information
    And map i_product_name to product_name using the product name

  Scenario: Create dimension table for stores
    Given target table "dim_store" with columns:
      | column_name     | data_type |
      | store_key       | NUMBER    |
      | store_id        | STRING    |
      | store_name      | STRING    |
      | city            | STRING    |
      | state           | STRING    |
      | market_desc     | STRING    |
      | division_name   | STRING    |
    
    When transforming from source to target
    Then map s_store_sk to store_key using the store surrogate key
    And map s_store_id to store_id using the store identifier
    And map s_store_name to store_name using the store name
    And map s_city to city using the store city location
    And map s_state to state using the store state location
    And map s_market_desc to market_desc using the market description
    And map s_division_name to division_name using the division name

  Scenario: Create dimension table for time
    Given target table "dim_date" with columns:
      | column_name | data_type |
      | date_key    | NUMBER    |
      | full_date   | DATE      |
      | year        | NUMBER    |
      | month       | NUMBER    |
      | month_seq   | NUMBER    |
    
    When transforming from source to target
    Then map d_date_sk to date_key using the date surrogate key
    And map d_date to full_date using the full date value
    And map d_year to year using the calendar year
    And map d_moy to month using the month of year
    And map d_month_seq to month_seq using the sequential month number