Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store per month

  Background: Source Schema
    Given the following source tables exist:
      | table_name | column_name | datatype |
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
      | store | s_country | STRING |
      | date_dim | d_date_sk | NUMBER |
      | date_dim | d_date | DATE |
      | date_dim | d_year | NUMBER |
      | date_dim | d_moy | NUMBER |
      | date_dim | d_month_seq | NUMBER |

  Scenario: Create dimension table for items
    Given source table "item"
    When I transform the data
    Then I create target table "dim_item" with schema:
      | column_name | datatype |
      | item_key | NUMBER |
      | item_id | STRING |
      | item_name | STRING |
      | item_description | STRING |
      | brand | STRING |
      | category | STRING |
    And I map the following fields:
      | source_field | target_field | transformation |
      | i_item_sk | item_key | direct mapping |
      | i_item_id | item_id | direct mapping |
      | i_product_name | item_name | direct mapping |
      | i_item_desc | item_description | direct mapping |
      | i_brand | brand | direct mapping |
      | i_category | category | direct mapping |

  Scenario: Create dimension table for stores
    Given source table "store"
    When I transform the data
    Then I create target table "dim_store" with schema:
      | column_name | datatype |
      | store_key | NUMBER |
      | store_id | STRING |
      | store_name | STRING |
      | city | STRING |
      | state | STRING |
      | country | STRING |
    And I map the following fields:
      | source_field | target_field | transformation |
      | s_store_sk | store_key | direct mapping |
      | s_store_id | store_id | direct mapping |
      | s_store_name | store_name | direct mapping |
      | s_city | city | direct mapping |
      | s_state | state | direct mapping |
      | s_country | country | direct mapping |

  Scenario: Create dimension table for time
    Given source table "date_dim"
    When I transform the data
    Then I create target table "dim_time" with schema:
      | column_name | datatype |
      | date_key | NUMBER |
      | full_date | DATE |
      | year | NUMBER |
      | month | NUMBER |
      | month_year | STRING |
      | month_seq | NUMBER |
    And I map the following fields:
      | source_field | target_field | transformation |
      | d_date_sk | date_key | direct mapping |
      | d_date | full_date | direct mapping |
      | d_year | year | direct mapping |
      | d_moy | month | direct mapping |
      | d_year and d_moy | month_year | concatenate year and month with hyphen |
      | d_month_seq | month_seq | direct mapping |

  Scenario: Create fact table for monthly sales
    Given source tables "store_sales", "item", "store", and "date_dim"
    When I join the tables using the following relationships:
      | left_table | left_column | right_table | right_column |
      | store_sales | ss_item_sk | item | i_item_sk |
      | store_sales | ss_store_sk | store | s_store_sk |
      | store_sales | ss_sold_date_sk | date_dim | d_date_sk |
    Then I create target table "fact_monthly_sales" with schema:
      | column_name | datatype |
      | date_key | NUMBER |
      | item_key | NUMBER |
      | store_key | NUMBER |
      | month_year | STRING |
      | total_quantity | NUMBER |
      | total_sales_amount | NUMBER |
      | total_net_paid | NUMBER |
      | transaction_count | NUMBER |
    And I map the following fields:
      | source_field | target_field | transformation |
      | d_date_sk | date_key | direct mapping |
      | i_item_sk | item_key | direct mapping |
      | s_store_sk | store_key | direct mapping |
      | d_year and d_moy | month_year | concatenate year and month with hyphen |
      | ss_quantity | total_quantity | sum by month, item, and store |
      | ss_ext_sales_price | total_sales_amount | sum by month, item, and store |
      | ss_net_paid | total_net_paid | sum by month, item, and store |
      | ss_sold_date_sk | transaction_count | count distinct by month, item, and store |
    And I aggregate data by month, item, and store