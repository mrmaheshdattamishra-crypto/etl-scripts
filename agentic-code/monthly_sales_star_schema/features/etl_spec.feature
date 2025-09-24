Feature: monthly_sales_star_schema
  As a business analyst
  I want to create a star schema for monthly sales analysis
  So that I can analyze sales by item/month and sales by store/month

  Background:
    Given the following source table schemas:
      | table_name  | column_name           | data_type    |
      | store_sales | ss_sold_date_sk       | NUMBER       |
      | store_sales | ss_item_sk            | NUMBER       |
      | store_sales | ss_store_sk           | NUMBER       |
      | store_sales | ss_quantity           | NUMBER       |
      | store_sales | ss_sales_price        | NUMBER       |
      | store_sales | ss_ext_sales_price    | NUMBER       |
      | store_sales | ss_net_paid           | NUMBER       |
      | item        | i_item_sk             | NUMBER       |
      | item        | i_item_id             | STRING       |
      | item        | i_item_desc           | STRING       |
      | item        | i_brand               | STRING       |
      | item        | i_category            | STRING       |
      | item        | i_product_name        | STRING       |
      | store       | s_store_sk            | NUMBER       |
      | store       | s_store_id            | STRING       |
      | store       | s_store_name          | STRING       |
      | store       | s_city                | STRING       |
      | store       | s_state               | STRING       |
      | date_dim    | d_date_sk             | NUMBER       |
      | date_dim    | d_date                | DATE         |
      | date_dim    | d_year                | NUMBER       |
      | date_dim    | d_moy                 | NUMBER       |
      | date_dim    | d_month_seq           | NUMBER       |

    And the following target table schemas:
      | table_name        | column_name       | data_type    |
      | fact_monthly_sales| date_key          | NUMBER       |
      | fact_monthly_sales| item_key          | NUMBER       |
      | fact_monthly_sales| store_key         | NUMBER       |
      | fact_monthly_sales| year_month        | STRING       |
      | fact_monthly_sales| total_quantity    | NUMBER       |
      | fact_monthly_sales| total_sales       | NUMBER       |
      | fact_monthly_sales| total_net_paid    | NUMBER       |
      | dim_item          | item_key          | NUMBER       |
      | dim_item          | item_id           | STRING       |
      | dim_item          | item_description  | STRING       |
      | dim_item          | brand             | STRING       |
      | dim_item          | category          | STRING       |
      | dim_item          | product_name      | STRING       |
      | dim_store         | store_key         | NUMBER       |
      | dim_store         | store_id          | STRING       |
      | dim_store         | store_name        | STRING       |
      | dim_store         | city              | STRING       |
      | dim_store         | state             | STRING       |
      | dim_date          | date_key          | NUMBER       |
      | dim_date          | date              | DATE         |
      | dim_date          | year              | NUMBER       |
      | dim_date          | month             | NUMBER       |
      | dim_date          | year_month        | STRING       |

  Scenario: Create dimension tables
    Given I have source data in item table
    When I transform the data
    Then I should create dim_item table where item_key is mapped from i_item_sk and item_id is mapped from i_item_id and item_description is mapped from i_item_desc and brand is mapped from i_brand and category is mapped from i_category and product_name is mapped from i_product_name

    Given I have source data in store table
    When I transform the data
    Then I should create dim_store table where store_key is mapped from s_store_sk and store_id is mapped from s_store_id and store_name is mapped from s_store_name and city is mapped from s_city and state is mapped from s_state

    Given I have source data in date_dim table
    When I transform the data
    Then I should create dim_date table where date_key is mapped from d_date_sk and date is mapped from d_date and year is mapped from d_year and month is mapped from d_moy and year_month is concatenated from d_year and d_moy with format YYYY-MM

  Scenario: Create fact table with monthly aggregation
    Given I have source data in store_sales table
    And I need to join with date_dim table on ss_sold_date_sk equals d_date_sk
    And I need to join with item table on ss_item_sk equals i_item_sk
    And I need to join with store table on ss_store_sk equals s_store_sk
    When I transform and aggregate the data by year and month and item and store
    Then I should create fact_monthly_sales table where date_key is mapped from d_date_sk and item_key is mapped from ss_item_sk and store_key is mapped from ss_store_sk and year_month is concatenated from d_year and d_moy with format YYYY-MM and total_quantity is sum of ss_quantity grouped by year month item and store and total_sales is sum of ss_ext_sales_price grouped by year month item and store and total_net_paid is sum of ss_net_paid grouped by year month item and store

  Scenario: Support KPI analysis
    Given I have the star schema with fact_monthly_sales as fact table
    And dim_item and dim_store and dim_date as dimension tables
    When analysts query for sales by item per month
    Then they can join fact_monthly_sales with dim_item and dim_date and aggregate total_sales by item and year_month

    When analysts query for sales by store per month
    Then they can join fact_monthly_sales with dim_store and dim_date and aggregate total_sales by store and year_month