# Data Product: monthly_sales_star_schema
# Description: Star schema for monthly sales analysis with KPIs for sales by item/month and sales by store/month

Feature: Monthly Sales Star Schema ETL
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store over time

  Background:
    Given source tables exist with schemas:
      | table_name  | columns                                                                    |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_ext_sales_price:NUMBER, ss_quantity:NUMBER, ss_net_paid:NUMBER |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING |

    And target star schema includes:
      | table_name      | columns                                                                    |
      | fact_monthly_sales | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, sales_amount:NUMBER, quantity_sold:NUMBER, net_paid:NUMBER, year_month:STRING |
      | dim_date_month  | date_key:NUMBER, year:NUMBER, month:NUMBER, month_name:STRING, year_month:STRING |
      | dim_item        | item_key:NUMBER, item_id:STRING, item_name:STRING, brand:STRING, category:STRING |
      | dim_store       | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING |

  Scenario: Create Date Dimension for Monthly Analysis
    Given the date_dim source table
    When transforming date dimension
    Then create dim_date_month with unique year-month combinations
    And map d_date_sk to date_key
    And map d_year to year
    And map d_moy to month
    And derive month_name from month number using case statement for Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec
    And concatenate year and month with hyphen to create year_month field

  Scenario: Create Item Dimension
    Given the item source table
    When transforming item dimension
    Then create dim_item with all active items
    And map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_product_name to item_name
    And map i_brand to brand
    And map i_category to category
    And filter for items where rec_end_date is null or greater than current date

  Scenario: Create Store Dimension
    Given the store source table
    When transforming store dimension
    Then create dim_store with all active stores
    And map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And filter for stores where rec_end_date is null or greater than current date

  Scenario: Create Monthly Sales Fact Table
    Given the store_sales fact table
    When joining with dimension tables
    Then join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And join store_sales to item on ss_item_sk equals i_item_sk
    And join store_sales to store on ss_store_sk equals s_store_sk
    When aggregating sales data by month
    Then group by d_date_sk, i_item_sk, s_store_sk, d_year, d_moy
    And sum ss_ext_sales_price as sales_amount
    And sum ss_quantity as quantity_sold
    And sum ss_net_paid as net_paid
    And create year_month field by concatenating d_year and d_moy with hyphen
    When creating fact_monthly_sales
    Then map d_date_sk to date_key
    And map i_item_sk to item_key
    And map s_store_sk to store_key
    And include aggregated sales_amount, quantity_sold, net_paid, and year_month

  Scenario: Support Sales by Item per Month KPI
    Given the fact_monthly_sales table exists
    When querying sales by item per month
    Then join fact_monthly_sales with dim_item on item_key
    And join with dim_date_month on date_key
    And group by item_name, year_month
    And sum sales_amount to get total sales per item per month

  Scenario: Support Sales by Store per Month KPI
    Given the fact_monthly_sales table exists
    When querying sales by store per month
    Then join fact_monthly_sales with dim_store on store_key
    And join with dim_date_month on date_key
    And group by store_name, year_month
    And sum sales_amount to get total sales per store per month