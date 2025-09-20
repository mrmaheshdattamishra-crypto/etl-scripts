Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data
  So that I can track sales performance by item and store

  Background:
    Given source tables with the following schemas:
      | table_name  | column_name           | datatype |
      | store_sales | ss_sold_date_sk       | NUMBER   |
      | store_sales | ss_item_sk            | NUMBER   |
      | store_sales | ss_store_sk           | NUMBER   |
      | store_sales | ss_quantity           | NUMBER   |
      | store_sales | ss_sales_price        | NUMBER   |
      | store_sales | ss_ext_sales_price    | NUMBER   |
      | store_sales | ss_net_paid           | NUMBER   |
      | item        | i_item_sk             | NUMBER   |
      | item        | i_item_id             | STRING   |
      | item        | i_product_name        | STRING   |
      | item        | i_brand               | STRING   |
      | item        | i_category            | STRING   |
      | store       | s_store_sk            | NUMBER   |
      | store       | s_store_id            | STRING   |
      | store       | s_store_name          | STRING   |
      | store       | s_city                | STRING   |
      | store       | s_state               | STRING   |
      | date_dim    | d_date_sk             | NUMBER   |
      | date_dim    | d_date                | DATE     |
      | date_dim    | d_year                | NUMBER   |
      | date_dim    | d_moy                 | NUMBER   |
      | date_dim    | d_month_seq           | NUMBER   |

  Scenario: Create dim_item dimension table
    Given the item source table
    When transforming item data
    Then create dim_item table with schema:
      | column_name      | datatype |
      | item_key         | NUMBER   |
      | item_id          | STRING   |
      | product_name     | STRING   |
      | brand            | STRING   |
      | category         | STRING   |
    And map item_key from i_item_sk
    And map item_id from i_item_id
    And map product_name from i_product_name
    And map brand from i_brand
    And map category from i_category

  Scenario: Create dim_store dimension table
    Given the store source table
    When transforming store data
    Then create dim_store table with schema:
      | column_name      | datatype |
      | store_key        | NUMBER   |
      | store_id         | STRING   |
      | store_name       | STRING   |
      | city             | STRING   |
      | state            | STRING   |
    And map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state

  Scenario: Create dim_date dimension table
    Given the date_dim source table
    When transforming date data
    Then create dim_date table with schema:
      | column_name      | datatype |
      | date_key         | NUMBER   |
      | full_date        | DATE     |
      | year             | NUMBER   |
      | month            | NUMBER   |
      | month_seq        | NUMBER   |
      | year_month       | STRING   |
    And map date_key from d_date_sk
    And map full_date from d_date
    And map year from d_year
    And map month from d_moy
    And map month_seq from d_month_seq
    And map year_month by concatenating year and month with dash separator

  Scenario: Create fact_sales fact table
    Given store_sales as the main fact table
    When joining with dimension tables
    Then create fact_sales table with schema:
      | column_name       | datatype |
      | date_key          | NUMBER   |
      | item_key          | NUMBER   |
      | store_key         | NUMBER   |
      | quantity          | NUMBER   |
      | sales_amount      | NUMBER   |
      | net_paid          | NUMBER   |
    And join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And join store_sales with item on ss_item_sk equals i_item_sk
    And join store_sales with store on ss_store_sk equals s_store_sk
    And map date_key from ss_sold_date_sk
    And map item_key from ss_item_sk
    And map store_key from ss_store_sk
    And map quantity from ss_quantity
    And map sales_amount from ss_ext_sales_price
    And map net_paid from ss_net_paid

  Scenario: Create monthly_sales_by_item aggregation
    Given fact_sales table joined with dim_date and dim_item
    When aggregating sales data by item and month
    Then create monthly_sales_by_item table with schema:
      | column_name       | datatype |
      | year_month        | STRING   |
      | item_key          | NUMBER   |
      | item_id           | STRING   |
      | product_name      | STRING   |
      | brand             | STRING   |
      | category          | STRING   |
      | total_quantity    | NUMBER   |
      | total_sales       | NUMBER   |
      | total_net_paid    | NUMBER   |
    And group by year_month, item_key, item_id, product_name, brand, category
    And sum quantity as total_quantity
    And sum sales_amount as total_sales
    And sum net_paid as total_net_paid

  Scenario: Create monthly_sales_by_store aggregation
    Given fact_sales table joined with dim_date and dim_store
    When aggregating sales data by store and month
    Then create monthly_sales_by_store table with schema:
      | column_name       | datatype |
      | year_month        | STRING   |
      | store_key         | NUMBER   |
      | store_id          | STRING   |
      | store_name        | STRING   |
      | city              | STRING   |
      | state             | STRING   |
      | total_quantity    | NUMBER   |
      | total_sales       | NUMBER   |
      | total_net_paid    | NUMBER   |
    And group by year_month, store_key, store_id, store_name, city, state
    And sum quantity as total_quantity
    And sum sales_amount as total_sales
    And sum net_paid as total_net_paid