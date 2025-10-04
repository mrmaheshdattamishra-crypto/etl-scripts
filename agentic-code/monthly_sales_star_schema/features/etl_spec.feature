# Referenced similar ticket SCRUM-9 with high similarity score (0.65)
# Adapting existing pattern for star schema monthly sales analysis

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data through a star schema
  So that I can track sales performance by item and store monthly

  Background: Source and Target Schema Definition
    Given the source tables with schema:
      | table_name  | column_name        | data_type    |
      | store_sales | ss_sold_date_sk    | NUMBER       |
      | store_sales | ss_item_sk         | NUMBER       |
      | store_sales | ss_store_sk        | NUMBER       |
      | store_sales | ss_quantity        | NUMBER       |
      | store_sales | ss_sales_price     | NUMBER       |
      | store_sales | ss_ext_sales_price | NUMBER       |
      | store_sales | ss_net_profit      | NUMBER       |
      | item        | i_item_sk          | NUMBER       |
      | item        | i_item_id          | STRING       |
      | item        | i_item_desc        | STRING       |
      | item        | i_brand            | STRING       |
      | item        | i_category         | STRING       |
      | item        | i_product_name     | STRING       |
      | store       | s_store_sk         | NUMBER       |
      | store       | s_store_id         | STRING       |
      | store       | s_store_name       | STRING       |
      | store       | s_city             | STRING       |
      | store       | s_state            | STRING       |
      | date_dim    | d_date_sk          | NUMBER       |
      | date_dim    | d_date             | DATE         |
      | date_dim    | d_year             | NUMBER       |
      | date_dim    | d_moy              | NUMBER       |
      | date_dim    | d_month_seq        | NUMBER       |

    And the target fact table schema:
      | table_name           | column_name      | data_type |
      | fact_monthly_sales   | date_key         | NUMBER    |
      | fact_monthly_sales   | item_key         | NUMBER    |
      | fact_monthly_sales   | store_key        | NUMBER    |
      | fact_monthly_sales   | year_month       | STRING    |
      | fact_monthly_sales   | total_quantity   | NUMBER    |
      | fact_monthly_sales   | total_sales      | NUMBER    |
      | fact_monthly_sales   | total_profit     | NUMBER    |
      | fact_monthly_sales   | transaction_count| NUMBER    |

    And the target dimension tables schema:
      | table_name    | column_name     | data_type |
      | dim_item      | item_key        | NUMBER    |
      | dim_item      | item_id         | STRING    |
      | dim_item      | item_name       | STRING    |
      | dim_item      | item_brand      | STRING    |
      | dim_item      | item_category   | STRING    |
      | dim_store     | store_key       | NUMBER    |
      | dim_store     | store_id        | STRING    |
      | dim_store     | store_name      | STRING    |
      | dim_store     | store_city      | STRING    |
      | dim_store     | store_state     | STRING    |
      | dim_date      | date_key        | NUMBER    |
      | dim_date      | full_date       | DATE      |
      | dim_date      | year            | NUMBER    |
      | dim_date      | month           | NUMBER    |
      | dim_date      | year_month      | STRING    |
      | dim_date      | month_sequence  | NUMBER    |

  Scenario: Create dimension tables for star schema
    When I process the item dimension
    Then dim_item should contain all unique items from item table with item_key as i_item_sk, item_id as i_item_id, item_name as i_product_name, item_brand as i_brand, and item_category as i_category

    When I process the store dimension
    Then dim_store should contain all unique stores from store table with store_key as s_store_sk, store_id as s_store_id, store_name as s_store_name, store_city as s_city, and store_state as s_state

    When I process the date dimension
    Then dim_date should contain all unique dates from date_dim table with date_key as d_date_sk, full_date as d_date, year as d_year, month as d_moy, year_month as concatenated d_year and d_moy with hyphen, and month_sequence as d_month_seq

  Scenario: Create monthly sales fact table
    Given the source tables are joined as follows:
      | join_type | left_table  | left_column     | right_table | right_column |
      | INNER     | store_sales | ss_sold_date_sk | date_dim    | d_date_sk    |
      | INNER     | store_sales | ss_item_sk      | item        | i_item_sk    |
      | INNER     | store_sales | ss_store_sk     | store       | s_store_sk   |

    When I aggregate sales data by year, month, item, and store
    Then fact_monthly_sales should contain date_key from d_date_sk, item_key from i_item_sk, store_key from s_store_sk, year_month as concatenated d_year and d_moy with hyphen, total_quantity as sum of ss_quantity, total_sales as sum of ss_ext_sales_price, total_profit as sum of ss_net_profit, and transaction_count as count of distinct ss_ticket_number

  Scenario: Enable KPI analysis for sales by item per month
    When I query fact_monthly_sales joined with dim_item and dim_date
    Then I can analyze total_sales, total_quantity, total_profit, and transaction_count grouped by item_name, item_brand, item_category, year, month, and year_month

  Scenario: Enable KPI analysis for sales by store per month
    When I query fact_monthly_sales joined with dim_store and dim_date
    Then I can analyze total_sales, total_quantity, total_profit, and transaction_count grouped by store_name, store_city, store_state, year, month, and year_month