# Data Product: monthly_sales_star_schema
# No similar tickets found in knowledge base - creating new pattern for sales analysis

Feature: Monthly Sales Star Schema
  As a business analyst
  I want to analyze monthly sales performance by item and store
  So that I can track sales KPIs and identify trends

  Background:
    Given the following source tables exist
      | table_name  | columns |
      | store_sales | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_paid (NUMBER), ss_net_profit (NUMBER) |
      | item        | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_class (STRING), i_category (STRING), i_product_name (STRING) |
      | store       | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_market_desc (STRING), s_division_name (STRING) |
      | date_dim    | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_month_seq (NUMBER), d_quarter_name (STRING) |

    And the target star schema includes
      | table_name | table_type | columns |
      | fact_monthly_sales | fact | date_key (NUMBER), item_key (NUMBER), store_key (NUMBER), sales_month (STRING), total_quantity (NUMBER), total_sales_amount (NUMBER), total_net_paid (NUMBER), total_net_profit (NUMBER), transaction_count (NUMBER) |
      | dim_item | dimension | item_key (NUMBER), item_id (STRING), item_description (STRING), brand (STRING), item_class (STRING), category (STRING), product_name (STRING) |
      | dim_store | dimension | store_key (NUMBER), store_id (STRING), store_name (STRING), city (STRING), state (STRING), market_description (STRING), division_name (STRING) |
      | dim_date_month | dimension | date_key (NUMBER), sales_month (STRING), year (NUMBER), month_number (NUMBER), quarter_name (STRING) |

  Scenario: Create dimension table for items
    Given I need to build dim_item from item table
    When I extract item dimension data
    Then map item_key from i_item_sk
    And map item_id from i_item_id
    And map item_description from i_item_desc
    And map brand from i_brand
    And map item_class from i_class
    And map category from i_category
    And map product_name from i_product_name

  Scenario: Create dimension table for stores
    Given I need to build dim_store from store table
    When I extract store dimension data
    Then map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state
    And map market_description from s_market_desc
    And map division_name from s_division_name

  Scenario: Create dimension table for monthly dates
    Given I need to build dim_date_month from date_dim table
    When I extract monthly date dimension data
    Then map date_key from d_date_sk
    And map sales_month by concatenating d_year and d_moy with dash separator
    And map year from d_year
    And map month_number from d_moy
    And map quarter_name from d_quarter_name

  Scenario: Create fact table for monthly sales aggregation
    Given I need to build fact_monthly_sales from store_sales table
    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I group by ss_item_sk, ss_store_sk, d_year, d_moy
    Then map date_key from d_date_sk using first occurrence in group
    And map item_key from ss_item_sk
    And map store_key from ss_store_sk
    And map sales_month by concatenating d_year and d_moy with dash separator
    And map total_quantity by summing ss_quantity
    And map total_sales_amount by summing ss_ext_sales_price
    And map total_net_paid by summing ss_net_paid
    And map total_net_profit by summing ss_net_profit
    And map transaction_count by counting distinct ss_ticket_number

  Scenario: Enable KPI analysis for sales by item per month
    Given fact_monthly_sales is joined with dim_item on item_key
    When I query sales by item and month
    Then I can aggregate total_sales_amount grouped by item_id, product_name, sales_month
    And I can calculate month over month growth for each item
    And I can rank top performing items by month

  Scenario: Enable KPI analysis for sales by store per month
    Given fact_monthly_sales is joined with dim_store on store_key
    When I query sales by store and month
    Then I can aggregate total_sales_amount grouped by store_name, city, state, sales_month
    And I can calculate month over month growth for each store
    And I can rank top performing stores by month