Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data
  So that I can track sales performance by item and store on a monthly basis

  Background:
    Given source tables with the following schemas:
      | table_name | column_name | data_type |
      | store_sales | ss_sold_date_sk | NUMBER |
      | store_sales | ss_item_sk | NUMBER |
      | store_sales | ss_store_sk | NUMBER |
      | store_sales | ss_quantity | NUMBER |
      | store_sales | ss_sales_price | NUMBER |
      | store_sales | ss_ext_sales_price | NUMBER |
      | store_sales | ss_net_paid | NUMBER |
      | item | i_item_sk | NUMBER |
      | item | i_item_id | STRING |
      | item | i_item_desc | STRING |
      | item | i_brand | STRING |
      | item | i_category | STRING |
      | item | i_product_name | STRING |
      | store | s_store_sk | NUMBER |
      | store | s_store_id | STRING |
      | store | s_store_name | STRING |
      | store | s_city | STRING |
      | store | s_state | STRING |
      | store | s_division_name | STRING |
      | date_dim | d_date_sk | NUMBER |
      | date_dim | d_date | DATE |
      | date_dim | d_year | NUMBER |
      | date_dim | d_moy | NUMBER |
      | date_dim | d_month_seq | NUMBER |

    And target data model with the following schemas:
      | table_name | column_name | data_type |
      | fact_monthly_sales | date_key | NUMBER |
      | fact_monthly_sales | item_key | NUMBER |
      | fact_monthly_sales | store_key | NUMBER |
      | fact_monthly_sales | year | NUMBER |
      | fact_monthly_sales | month | NUMBER |
      | fact_monthly_sales | total_quantity | NUMBER |
      | fact_monthly_sales | total_sales_amount | NUMBER |
      | fact_monthly_sales | total_net_paid | NUMBER |
      | dim_item | item_key | NUMBER |
      | dim_item | item_id | STRING |
      | dim_item | item_description | STRING |
      | dim_item | brand | STRING |
      | dim_item | category | STRING |
      | dim_item | product_name | STRING |
      | dim_store | store_key | NUMBER |
      | dim_store | store_id | STRING |
      | dim_store | store_name | STRING |
      | dim_store | city | STRING |
      | dim_store | state | STRING |
      | dim_store | division_name | STRING |
      | dim_date | date_key | NUMBER |
      | dim_date | full_date | DATE |
      | dim_date | year | NUMBER |
      | dim_date | month | NUMBER |
      | dim_date | month_sequence | NUMBER |

  Scenario: Create dimension table for items
    Given the item source table
    When transforming item data
    Then map item_key from i_item_sk
    And map item_id from i_item_id
    And map item_description from i_item_desc
    And map brand from i_brand
    And map category from i_category
    And map product_name from i_product_name
    And load into dim_item table

  Scenario: Create dimension table for stores
    Given the store source table
    When transforming store data
    Then map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state
    And map division_name from s_division_name
    And load into dim_store table

  Scenario: Create dimension table for dates
    Given the date_dim source table
    When transforming date data
    Then map date_key from d_date_sk
    And map full_date from d_date
    And map year from d_year
    And map month from d_moy
    And map month_sequence from d_month_seq
    And load into dim_date table

  Scenario: Create fact table for monthly sales
    Given the store_sales source table
    And join with date_dim table on ss_sold_date_sk equals d_date_sk
    And join with item table on ss_item_sk equals i_item_sk
    And join with store table on ss_store_sk equals s_store_sk
    When aggregating sales data by year and month and item and store
    Then map date_key from d_date_sk
    And map item_key from ss_item_sk
    And map store_key from ss_store_sk
    And map year from d_year
    And map month from d_moy
    And calculate total_quantity as sum of ss_quantity
    And calculate total_sales_amount as sum of ss_ext_sales_price
    And calculate total_net_paid as sum of ss_net_paid
    And group by year and month and item_key and store_key
    And load into fact_monthly_sales table