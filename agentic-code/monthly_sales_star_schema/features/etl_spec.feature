Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data
  So that I can track sales performance by item and store

  Background:
    Given the following source table schemas:
      | table_name  | column_name          | data_type    |
      | store_sales | ss_sold_date_sk      | NUMBER       |
      | store_sales | ss_item_sk           | NUMBER       |
      | store_sales | ss_store_sk          | NUMBER       |
      | store_sales | ss_quantity          | NUMBER       |
      | store_sales | ss_sales_price       | NUMBER       |
      | store_sales | ss_ext_sales_price   | NUMBER       |
      | store_sales | ss_net_paid          | NUMBER       |
      | item        | i_item_sk            | NUMBER       |
      | item        | i_item_id            | STRING       |
      | item        | i_item_desc          | STRING       |
      | item        | i_brand              | STRING       |
      | item        | i_category           | STRING       |
      | item        | i_product_name       | STRING       |
      | store       | s_store_sk           | NUMBER       |
      | store       | s_store_id           | STRING       |
      | store       | s_store_name         | STRING       |
      | store       | s_city               | STRING       |
      | store       | s_state              | STRING       |
      | date_dim    | d_date_sk            | NUMBER       |
      | date_dim    | d_date               | DATE         |
      | date_dim    | d_year               | NUMBER       |
      | date_dim    | d_moy                | NUMBER       |
      | date_dim    | d_month_seq          | NUMBER       |

    And the following target table schemas:
      | table_name       | column_name       | data_type    |
      | fact_sales       | date_key          | NUMBER       |
      | fact_sales       | item_key          | NUMBER       |
      | fact_sales       | store_key         | NUMBER       |
      | fact_sales       | year_month        | STRING       |
      | fact_sales       | quantity          | NUMBER       |
      | fact_sales       | sales_amount      | NUMBER       |
      | fact_sales       | net_paid          | NUMBER       |
      | dim_item         | item_key          | NUMBER       |
      | dim_item         | item_id           | STRING       |
      | dim_item         | item_description  | STRING       |
      | dim_item         | brand             | STRING       |
      | dim_item         | category          | STRING       |
      | dim_item         | product_name      | STRING       |
      | dim_store        | store_key         | NUMBER       |
      | dim_store        | store_id          | STRING       |
      | dim_store        | store_name        | STRING       |
      | dim_store        | city              | STRING       |
      | dim_store        | state             | STRING       |
      | dim_date         | date_key          | NUMBER       |
      | dim_date         | date_value        | DATE         |
      | dim_date         | year              | NUMBER       |
      | dim_date         | month             | NUMBER       |
      | dim_date         | year_month        | STRING       |

  Scenario: Create dimension tables
    Given I have source data in item, store, and date_dim tables
    When I transform the data for dimension tables
    Then I should create dim_item table by selecting item key, item id, item description, brand, category, and product name from item table
    And I should create dim_store table by selecting store key, store id, store name, city, and state from store table
    And I should create dim_date table by selecting date key, date value, year, month, and concatenating year and month as year_month from date_dim table

  Scenario: Create fact table with monthly aggregation
    Given I have source data in store_sales table
    And I have dimension tables created
    When I transform the sales data for fact table
    Then I should join store_sales with date_dim on sold date key equals date key
    And I should join store_sales with item on item key equals item key
    And I should join store_sales with store on store key equals store key
    And I should group by date key, item key, store key, and year_month
    And I should sum quantity as total quantity
    And I should sum extended sales price as total sales amount
    And I should sum net paid as total net paid
    And I should create fact_sales table with aggregated monthly sales data

  Scenario: Enable sales by item per month analysis
    Given I have fact_sales and dim_item tables
    When I analyze sales by item and month
    Then I should join fact_sales with dim_item on item key
    And I should group by item description, brand, category, product name, and year_month
    And I should sum sales amount and quantity for each item per month

  Scenario: Enable sales by store per month analysis
    Given I have fact_sales and dim_store tables
    When I analyze sales by store and month
    Then I should join fact_sales with dim_store on store key
    And I should group by store name, city, state, and year_month
    And I should sum sales amount and quantity for each store per month