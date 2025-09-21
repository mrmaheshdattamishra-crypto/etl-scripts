Feature: monthly_sales_analysis
  As a business analyst
  I want to create a star schema for monthly sales analysis
  So that I can analyze sales by item/month and sales by store/month

  Background: Source Schema
    Given the following source tables exist:
      | table_name  | columns                                                                                                    |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_market_desc:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER |

  Scenario: Create fact table for monthly sales
    Given I have access to store_sales as the main fact source
    When I create the fact_monthly_sales table
    Then the target schema should be:
      | column_name        | data_type | description |
      | date_key          | NUMBER    | Foreign key to date dimension |
      | item_key          | NUMBER    | Foreign key to item dimension |
      | store_key         | NUMBER    | Foreign key to store dimension |
      | year_month        | STRING    | Year and month in YYYY-MM format |
      | total_quantity    | NUMBER    | Sum of quantities sold |
      | total_sales       | NUMBER    | Sum of extended sales price |
      | total_net_paid    | NUMBER    | Sum of net paid amount |
      | avg_sales_price   | NUMBER    | Average sales price |
    And the data should be aggregated by joining store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And group the results by d_year, d_moy, ss_item_sk, and ss_store_sk
    And calculate sum of ss_quantity as total_quantity
    And calculate sum of ss_ext_sales_price as total_sales  
    And calculate sum of ss_net_paid as total_net_paid
    And calculate average of ss_sales_price as avg_sales_price
    And create year_month by concatenating d_year and d_moy with hyphen separator

  Scenario: Create item dimension
    Given I have access to item table
    When I create the dim_item table
    Then the target schema should be:
      | column_name     | data_type | description |
      | item_key       | NUMBER    | Primary key from source |
      | item_id        | STRING    | Item identifier |
      | item_desc      | STRING    | Item description |
      | brand          | STRING    | Item brand |
      | class          | STRING    | Item class |
      | category       | STRING    | Item category |
      | product_name   | STRING    | Product name |
    And select i_item_sk as item_key
    And select i_item_id as item_id
    And select i_item_desc as item_desc
    And select i_brand as brand
    And select i_class as class
    And select i_category as category
    And select i_product_name as product_name

  Scenario: Create store dimension
    Given I have access to store table
    When I create the dim_store table
    Then the target schema should be:
      | column_name    | data_type | description |
      | store_key     | NUMBER    | Primary key from source |
      | store_id      | STRING    | Store identifier |
      | store_name    | STRING    | Store name |
      | city          | STRING    | Store city |
      | state         | STRING    | Store state |
      | market_desc   | STRING    | Market description |
    And select s_store_sk as store_key
    And select s_store_id as store_id
    And select s_store_name as store_name
    And select s_city as city
    And select s_state as state
    And select s_market_desc as market_desc

  Scenario: Create date dimension
    Given I have access to date_dim table
    When I create the dim_date table
    Then the target schema should be:
      | column_name   | data_type | description |
      | date_key     | NUMBER    | Primary key from source |
      | calendar_date| DATE      | Calendar date |
      | year         | NUMBER    | Year |
      | month        | NUMBER    | Month of year |
      | month_seq    | NUMBER    | Month sequence |
      | year_month   | STRING    | Year and month in YYYY-MM format |
    And select d_date_sk as date_key
    And select d_date as calendar_date
    And select d_year as year
    And select d_moy as month
    And select d_month_seq as month_seq
    And create year_month by concatenating d_year and d_moy with hyphen separator

  Scenario: Define relationships for star schema
    Given the fact table fact_monthly_sales exists
    And the dimension tables dim_item, dim_store, and dim_date exist
    Then establish the following relationships:
      | fact_table        | fact_column | dimension_table | dimension_column |
      | fact_monthly_sales| item_key    | dim_item       | item_key        |
      | fact_monthly_sales| store_key   | dim_store      | store_key       |
      | fact_monthly_sales| date_key    | dim_date       | date_key        |