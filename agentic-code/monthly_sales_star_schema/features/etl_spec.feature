Feature: monthly_sales_star_schema
  As a data analyst
  I want to analyze monthly sales performance 
  So that I can track sales by item and store on a monthly basis

  Background: Source and Target Schema Definition
    Given the following source tables:
      | table_name    | columns |
      | catalog_sales | cs_sold_date_sk (NUMBER), cs_item_sk (NUMBER), cs_call_center_sk (NUMBER), cs_quantity (NUMBER), cs_sales_price (NUMBER), cs_ext_sales_price (NUMBER), cs_net_profit (NUMBER) |
      | store_sales   | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_profit (NUMBER) |
      | web_sales     | ws_sold_date_sk (NUMBER), ws_item_sk (NUMBER), ws_web_site_sk (NUMBER), ws_quantity (NUMBER), ws_sales_price (NUMBER), ws_ext_sales_price (NUMBER), ws_net_profit (NUMBER) |
      | item          | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_category (STRING), i_class (STRING) |
      | store         | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_country (STRING) |
      | date_dim      | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_month_seq (NUMBER) |

    And the following target star schema:
      | table_name           | columns |
      | fact_monthly_sales   | date_key (NUMBER), item_key (NUMBER), store_key (NUMBER), channel (STRING), total_quantity (NUMBER), total_sales_amount (NUMBER), total_profit (NUMBER) |
      | dim_item             | item_key (NUMBER), item_id (STRING), item_description (STRING), brand (STRING), category (STRING), class (STRING) |
      | dim_store            | store_key (NUMBER), store_id (STRING), store_name (STRING), city (STRING), state (STRING), country (STRING) |
      | dim_date_month       | date_key (NUMBER), year (NUMBER), month (NUMBER), year_month (STRING) |

  Scenario: Create dimension tables
    Given I need to create item dimension
    When I extract data from item table
    Then I should map item_key to i_item_sk
    And I should map item_id to i_item_id
    And I should map item_description to i_item_desc
    And I should map brand to i_brand
    And I should map category to i_category
    And I should map class to i_class

    Given I need to create store dimension  
    When I extract data from store table
    Then I should map store_key to s_store_sk
    And I should map store_id to s_store_id
    And I should map store_name to s_store_name
    And I should map city to s_city
    And I should map state to s_state
    And I should map country to s_country

    Given I need to create monthly date dimension
    When I extract data from date_dim table
    Then I should map date_key to d_date_sk
    And I should map year to d_year
    And I should map month to d_moy
    And I should create year_month by concatenating d_year and d_moy with hyphen

  Scenario: Create fact table for monthly sales aggregation
    Given I need to aggregate sales data by item and month from multiple channels
    When I join catalog_sales with date_dim on cs_sold_date_sk equals d_date_sk
    And I join the result with item on cs_item_sk equals i_item_sk
    Then I should group by d_date_sk, i_item_sk, and set channel to catalog
    And I should sum cs_quantity as total_quantity
    And I should sum cs_ext_sales_price as total_sales_amount
    And I should sum cs_net_profit as total_profit
    And I should map date_key to d_date_sk
    And I should map item_key to i_item_sk
    And I should set store_key to null for catalog sales

    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I join the result with item on ss_item_sk equals i_item_sk  
    And I join the result with store on ss_store_sk equals s_store_sk
    Then I should group by d_date_sk, i_item_sk, s_store_sk and set channel to store
    And I should sum ss_quantity as total_quantity
    And I should sum ss_ext_sales_price as total_sales_amount
    And I should sum ss_net_profit as total_profit
    And I should map date_key to d_date_sk
    And I should map item_key to i_item_sk
    And I should map store_key to s_store_sk

    When I join web_sales with date_dim on ws_sold_date_sk equals d_date_sk
    And I join the result with item on ws_item_sk equals i_item_sk
    Then I should group by d_date_sk, i_item_sk and set channel to web
    And I should sum ws_quantity as total_quantity
    And I should sum ws_ext_sales_price as total_sales_amount  
    And I should sum ws_net_profit as total_profit
    And I should map date_key to d_date_sk
    And I should map item_key to i_item_sk
    And I should set store_key to null for web sales

    Then I should union all three channel datasets into fact_monthly_sales table

  Scenario: Support KPI queries
    Given the star schema is populated
    When I need sales by item per month
    Then I should join fact_monthly_sales with dim_item on item_key
    And I should join with dim_date_month on date_key
    And I should group by item_description, year, month
    And I should sum total_sales_amount

    When I need sales by store per month  
    Then I should join fact_monthly_sales with dim_store on store_key
    And I should join with dim_date_month on date_key
    And I should filter where store_key is not null
    And I should group by store_name, year, month
    And I should sum total_sales_amount