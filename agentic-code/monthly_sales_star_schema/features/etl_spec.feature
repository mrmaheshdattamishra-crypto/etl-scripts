# Referenced similar ticket SCRUM-9 with identical requirements for monthly sales star schema
# Leveraging existing patterns for dimensional modeling and KPI calculations

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background: Source and Target Schema
    Given the source tables:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_division_name:STRING, s_company_name:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER, d_quarter_name:STRING |
    
    And the target star schema tables:
      | table_name      | columns |
      | fact_monthly_sales | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, sales_month:STRING, total_quantity:NUMBER, total_sales_amount:NUMBER, total_net_paid:NUMBER, avg_sales_price:NUMBER |
      | dim_item_monthly   | item_key:NUMBER, item_id:STRING, item_description:STRING, brand:STRING, class:STRING, category:STRING, product_name:STRING |
      | dim_store_monthly  | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, division:STRING, company:STRING |
      | dim_date_monthly   | date_key:NUMBER, sales_month:STRING, year:NUMBER, month_number:NUMBER, month_sequence:NUMBER, quarter:STRING |

  Scenario: Build dimensional tables for monthly sales analysis
    Given I want to create dimension tables for the star schema
    When I process the item dimension
    Then I select all distinct items from item table
    And I map item_key to i_item_sk
    And I map item_id to i_item_id
    And I map item_description to i_item_desc
    And I map brand to i_brand
    And I map class to i_class
    And I map category to i_category
    And I map product_name to i_product_name

    When I process the store dimension
    Then I select all distinct stores from store table
    And I map store_key to s_store_sk
    And I map store_id to s_store_id
    And I map store_name to s_store_name
    And I map city to s_city
    And I map state to s_state
    And I map division to s_division_name
    And I map company to s_company_name

    When I process the date dimension
    Then I select distinct year-month combinations from date_dim table
    And I map date_key to d_date_sk
    And I create sales_month by concatenating d_year and d_moy with hyphen
    And I map year to d_year
    And I map month_number to d_moy
    And I map month_sequence to d_month_seq
    And I map quarter to d_quarter_name

  Scenario: Build fact table for monthly sales aggregation
    Given I want to create the monthly sales fact table
    When I join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And I join to item on ss_item_sk equals i_item_sk
    And I join to store on ss_store_sk equals s_store_sk
    Then I group by year-month combination, item_sk, and store_sk
    And I map date_key to d_date_sk from the first record in each month group
    And I map item_key to ss_item_sk
    And I map store_key to ss_store_sk
    And I create sales_month by concatenating d_year and d_moy with hyphen
    And I calculate total_quantity as sum of ss_quantity
    And I calculate total_sales_amount as sum of ss_ext_sales_price
    And I calculate total_net_paid as sum of ss_net_paid
    And I calculate avg_sales_price as average of ss_sales_price

  Scenario: Enable KPI analysis for sales by item per month
    Given the monthly sales star schema is built
    When I want to analyze sales by item per month
    Then I can join fact_monthly_sales to dim_item_monthly on item_key
    And I can join to dim_date_monthly on date_key
    And I can aggregate total_sales_amount and total_quantity by item and sales_month
    And I can filter and group by item attributes like brand, category, class
    And I can trend analysis across multiple months for each item

  Scenario: Enable KPI analysis for sales by store per month  
    Given the monthly sales star schema is built
    When I want to analyze sales by store per month
    Then I can join fact_monthly_sales to dim_store_monthly on store_key
    And I can join to dim_date_monthly on date_key
    And I can aggregate total_sales_amount and total_quantity by store and sales_month
    And I can filter and group by store attributes like city, state, division
    And I can compare performance across stores within the same month
    And I can trend analysis across multiple months for each store