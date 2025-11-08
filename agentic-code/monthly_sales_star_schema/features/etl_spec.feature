Feature: monthly_sales_star_schema
  # Referenced similar ticket SCRUM-9 with nearly identical requirements
  # Building upon existing store_sales, item, store, and date_dim tables pattern

  Scenario: Source Schema Definition
    Given the source tables with the following schemas:
      | Table Name | Column Name | Data Type |
      | store_sales | ss_sold_date_sk | NUMBER |
      | store_sales | ss_item_sk | NUMBER |
      | store_sales | ss_store_sk | NUMBER |
      | store_sales | ss_quantity | NUMBER |
      | store_sales | ss_sales_price | NUMBER |
      | store_sales | ss_ext_sales_price | NUMBER |
      | store_sales | ss_net_paid | NUMBER |
      | store_sales | ss_net_profit | NUMBER |
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
      | store | s_market_desc | STRING |
      | date_dim | d_date_sk | NUMBER |
      | date_dim | d_date | DATE |
      | date_dim | d_year | NUMBER |
      | date_dim | d_moy | NUMBER |
      | date_dim | d_month_seq | NUMBER |

  Scenario: Target Schema Definition
    Given the target star schema with the following tables:
      | Table Name | Column Name | Data Type |
      | fact_monthly_sales | date_key | NUMBER |
      | fact_monthly_sales | item_key | NUMBER |
      | fact_monthly_sales | store_key | NUMBER |
      | fact_monthly_sales | sales_month | NUMBER |
      | fact_monthly_sales | sales_year | NUMBER |
      | fact_monthly_sales | total_quantity | NUMBER |
      | fact_monthly_sales | total_sales_amount | NUMBER |
      | fact_monthly_sales | total_net_paid | NUMBER |
      | fact_monthly_sales | total_net_profit | NUMBER |
      | fact_monthly_sales | transaction_count | NUMBER |
      | dim_item | item_key | NUMBER |
      | dim_item | item_id | STRING |
      | dim_item | item_description | STRING |
      | dim_item | brand_name | STRING |
      | dim_item | category_name | STRING |
      | dim_item | product_name | STRING |
      | dim_store | store_key | NUMBER |
      | dim_store | store_id | STRING |
      | dim_store | store_name | STRING |
      | dim_store | store_city | STRING |
      | dim_store | store_state | STRING |
      | dim_store | market_description | STRING |
      | dim_date_monthly | date_key | NUMBER |
      | dim_date_monthly | year_month | STRING |
      | dim_date_monthly | sales_year | NUMBER |
      | dim_date_monthly | sales_month | NUMBER |
      | dim_date_monthly | month_name | STRING |

  Scenario: Table Relationships
    Given the following join relationships:
      | Source Table | Join Column | Target Table | Join Column |
      | store_sales | ss_sold_date_sk | date_dim | d_date_sk |
      | store_sales | ss_item_sk | item | i_item_sk |
      | store_sales | ss_store_sk | store | s_store_sk |

  Scenario: Dimension Table ETL - Item Dimension
    When creating the item dimension table
    Then extract item_key from item table i_item_sk
    And extract item_id from item table i_item_id
    And extract item_description from item table i_item_desc
    And extract brand_name from item table i_brand
    And extract category_name from item table i_category
    And extract product_name from item table i_product_name
    And load all records from item table to dim_item

  Scenario: Dimension Table ETL - Store Dimension
    When creating the store dimension table
    Then extract store_key from store table s_store_sk
    And extract store_id from store table s_store_id
    And extract store_name from store table s_store_name
    And extract store_city from store table s_city
    And extract store_state from store table s_state
    And extract market_description from store table s_market_desc
    And load all records from store table to dim_store

  Scenario: Dimension Table ETL - Monthly Date Dimension
    When creating the monthly date dimension table
    Then extract unique combinations of year and month from date_dim table
    And create date_key as concatenation of d_year and d_moy with zero padding
    And create year_month as concatenation of d_year and month name
    And extract sales_year from d_year
    And extract sales_month from d_moy
    And convert sales_month to month_name using month number to name mapping
    And load unique year-month combinations to dim_date_monthly

  Scenario: Fact Table ETL - Monthly Sales Aggregation
    When creating the monthly sales fact table
    Then join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And group by d_year, d_moy, ss_item_sk, and ss_store_sk
    And create date_key as concatenation of d_year and d_moy with zero padding
    And set item_key as ss_item_sk
    And set store_key as ss_store_sk
    And set sales_year as d_year
    And set sales_month as d_moy
    And calculate total_quantity as sum of ss_quantity
    And calculate total_sales_amount as sum of ss_ext_sales_price
    And calculate total_net_paid as sum of ss_net_paid
    And calculate total_net_profit as sum of ss_net_profit
    And calculate transaction_count as count of distinct ss_ticket_number
    And load aggregated results to fact_monthly_sales