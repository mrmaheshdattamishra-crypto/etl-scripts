# Similar ticket reference: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Leveraging existing star schema pattern for monthly sales analysis

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance 
  So that I can track sales by item and store over time

  Background: Source Tables Schema
    Given source table "store_sales" with schema:
      | column_name          | data_type    |
      | ss_sold_date_sk      | NUMBER       |
      | ss_item_sk           | NUMBER       |
      | ss_store_sk          | NUMBER       |
      | ss_quantity          | NUMBER       |
      | ss_sales_price       | NUMBER       |
      | ss_ext_sales_price   | NUMBER       |
      | ss_net_paid          | NUMBER       |
      | ss_net_profit        | NUMBER       |
    
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
      | column_name      | data_type |
      | s_store_sk       | NUMBER    |
      | s_store_id       | STRING    |
      | s_store_name     | STRING    |
      | s_city           | STRING    |
      | s_state          | STRING    |
      | s_division_name  | STRING    |
      | s_company_name   | STRING    |
    
    And source table "date_dim" with schema:
      | column_name | data_type |
      | d_date_sk   | NUMBER    |
      | d_date      | DATE      |
      | d_year      | NUMBER    |
      | d_moy       | NUMBER    |
      | d_month_seq | NUMBER    |

  Scenario: Create fact table for monthly sales
    Given target table "fact_monthly_sales" with schema:
      | column_name        | data_type |
      | date_key           | NUMBER    |
      | item_key           | NUMBER    |
      | store_key          | NUMBER    |
      | year               | NUMBER    |
      | month              | NUMBER    |
      | month_seq          | NUMBER    |
      | total_quantity     | NUMBER    |
      | total_sales_amount | NUMBER    |
      | total_net_paid     | NUMBER    |
      | total_net_profit   | NUMBER    |
      | transaction_count  | NUMBER    |
    
    When extracting from source tables
    Then join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And aggregate store_sales data by d_year, d_moy, d_month_seq, ss_item_sk, and ss_store_sk
    And map d_date_sk to date_key
    And map ss_item_sk to item_key  
    And map ss_store_sk to store_key
    And map d_year to year
    And map d_moy to month
    And map d_month_seq to month_seq
    And map sum of ss_quantity to total_quantity
    And map sum of ss_ext_sales_price to total_sales_amount
    And map sum of ss_net_paid to total_net_paid
    And map sum of ss_net_profit to total_net_profit
    And map count of distinct ss_ticket_number to transaction_count

  Scenario: Create dimension table for items
    Given target table "dim_item" with schema:
      | column_name  | data_type |
      | item_key     | NUMBER    |
      | item_id      | STRING    |
      | item_desc    | STRING    |
      | brand        | STRING    |
      | category     | STRING    |
      | class        | STRING    |
      | product_name | STRING    |
    
    When extracting from item table
    Then map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_item_desc to item_desc
    And map i_brand to brand
    And map i_category to category
    And map i_class to class
    And map i_product_name to product_name

  Scenario: Create dimension table for stores
    Given target table "dim_store" with schema:
      | column_name   | data_type |
      | store_key     | NUMBER    |
      | store_id      | STRING    |
      | store_name    | STRING    |
      | city          | STRING    |
      | state         | STRING    |
      | division_name | STRING    |
      | company_name  | STRING    |
    
    When extracting from store table
    Then map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And map s_division_name to division_name
    And map s_company_name to company_name

  Scenario: Create dimension table for time periods
    Given target table "dim_date" with schema:
      | column_name  | data_type |
      | date_key     | NUMBER    |
      | date         | DATE      |
      | year         | NUMBER    |
      | month        | NUMBER    |
      | month_seq    | NUMBER    |
    
    When extracting from date_dim table
    Then map d_date_sk to date_key
    And map d_date to date
    And map d_year to year
    And map d_moy to month
    And map d_month_seq to month_seq