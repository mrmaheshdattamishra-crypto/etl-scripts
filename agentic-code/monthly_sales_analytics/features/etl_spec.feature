# ETL Specification for Monthly Sales Star Schema
# Referenced similar ticket: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Leveraging existing star schema patterns for sales analysis

Feature: monthly_sales_analytics
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track KPIs for sales by item and store per month

  Background: Source Schema
    Given source table "store_sales" with schema:
      | column_name           | data_type     |
      | ss_sold_date_sk      | NUMBER        |
      | ss_item_sk           | NUMBER        |
      | ss_store_sk          | NUMBER        |
      | ss_sales_price       | NUMBER        |
      | ss_ext_sales_price   | NUMBER        |
      | ss_quantity          | NUMBER        |
      | ss_net_profit        | NUMBER        |

    And source table "item" with schema:
      | column_name     | data_type |
      | i_item_sk      | NUMBER    |
      | i_item_id      | STRING    |
      | i_item_desc    | STRING    |
      | i_brand        | STRING    |
      | i_category     | STRING    |
      | i_product_name | STRING    |

    And source table "store" with schema:
      | column_name    | data_type |
      | s_store_sk    | NUMBER    |
      | s_store_id    | STRING    |
      | s_store_name  | STRING    |
      | s_city        | STRING    |
      | s_state       | STRING    |

    And source table "date_dim" with schema:
      | column_name | data_type |
      | d_date_sk  | NUMBER    |
      | d_date     | DATE      |
      | d_year     | NUMBER    |
      | d_moy      | NUMBER    |

  Scenario: Create fact table for monthly sales
    Given target table "fact_monthly_sales" with schema:
      | column_name        | data_type |
      | date_key          | NUMBER    |
      | item_key          | NUMBER    |
      | store_key         | NUMBER    |
      | sales_month       | NUMBER    |
      | sales_year        | NUMBER    |
      | total_sales       | NUMBER    |
      | total_quantity    | NUMBER    |
      | total_profit      | NUMBER    |
      | transaction_count | NUMBER    |

    When I join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    Then I aggregate sales data by d_date_sk, ss_item_sk, ss_store_sk, d_year, and d_moy
    And I sum ss_ext_sales_price as total_sales
    And I sum ss_quantity as total_quantity
    And I sum ss_net_profit as total_profit
    And I count distinct ss_ticket_number as transaction_count
    And I map d_date_sk to date_key
    And I map ss_item_sk to item_key
    And I map ss_store_sk to store_key
    And I map d_moy to sales_month
    And I map d_year to sales_year

  Scenario: Create item dimension table
    Given target table "dim_item" with schema:
      | column_name  | data_type |
      | item_key    | NUMBER    |
      | item_id     | STRING    |
      | item_name   | STRING    |
      | brand       | STRING    |
      | category    | STRING    |
      | description | STRING    |

    When I select from item table
    Then I map i_item_sk to item_key
    And I map i_item_id to item_id
    And I map i_product_name to item_name
    And I map i_brand to brand
    And I map i_category to category
    And I map i_item_desc to description

  Scenario: Create store dimension table
    Given target table "dim_store" with schema:
      | column_name | data_type |
      | store_key  | NUMBER    |
      | store_id   | STRING    |
      | store_name | STRING    |
      | city       | STRING    |
      | state      | STRING    |

    When I select from store table
    Then I map s_store_sk to store_key
    And I map s_store_id to store_id
    And I map s_store_name to store_name
    And I map s_city to city
    And I map s_state to state

  Scenario: Create date dimension table
    Given target table "dim_date" with schema:
      | column_name | data_type |
      | date_key   | NUMBER    |
      | date_value | DATE      |
      | year       | NUMBER    |
      | month      | NUMBER    |

    When I select from date_dim table
    Then I map d_date_sk to date_key
    And I map d_date to date_value
    And I map d_year to year
    And I map d_moy to month