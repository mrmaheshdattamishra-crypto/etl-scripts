Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store per month

  Background:
    Given the following source tables:
      | table_name  | columns                                                                                                                                                                                                    |
      | store_sales | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_paid (NUMBER), ss_net_profit (NUMBER) |
      | item        | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_class (STRING), i_category (STRING), i_product_name (STRING)                                                    |
      | store       | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_market_desc (STRING), s_division_name (STRING)                                              |
      | date_dim    | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_quarter_name (STRING), d_month_seq (NUMBER)                                                                                  |

    And the target star schema tables:
      | table_name         | columns                                                                                                                                                   |
      | fact_monthly_sales | date_key (NUMBER), item_key (NUMBER), store_key (NUMBER), sales_amount (NUMBER), quantity_sold (NUMBER), net_profit (NUMBER), year_month (STRING)     |
      | dim_item           | item_key (NUMBER), item_id (STRING), item_name (STRING), item_description (STRING), brand (STRING), class (STRING), category (STRING)                |
      | dim_store          | store_key (NUMBER), store_id (STRING), store_name (STRING), city (STRING), state (STRING), market_description (STRING), division_name (STRING)       |
      | dim_date           | date_key (NUMBER), date_value (DATE), year (NUMBER), month (NUMBER), quarter_name (STRING), year_month (STRING), month_sequence (NUMBER)             |

  Scenario: Create dimension table for items
    Given source table item
    When transforming to dim_item
    Then map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_product_name to item_name
    And map i_item_desc to item_description
    And map i_brand to brand
    And map i_class to class
    And map i_category to category

  Scenario: Create dimension table for stores
    Given source table store
    When transforming to dim_store
    Then map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And map s_market_desc to market_description
    And map s_division_name to division_name

  Scenario: Create dimension table for dates
    Given source table date_dim
    When transforming to dim_date
    Then map d_date_sk to date_key
    And map d_date to date_value
    And map d_year to year
    And map d_moy to month
    And map d_quarter_name to quarter_name
    And concatenate d_year and d_moy with hyphen separator to create year_month
    And map d_month_seq to month_sequence

  Scenario: Create fact table for monthly sales
    Given source tables store_sales, item, store, and date_dim
    When joining store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And joining store_sales to item on ss_item_sk equals i_item_sk
    And joining store_sales to store on ss_store_sk equals s_store_sk
    Then group by d_date_sk, i_item_sk, s_store_sk, d_year, and d_moy
    And map d_date_sk to date_key
    And map i_item_sk to item_key
    And map s_store_sk to store_key
    And sum ss_ext_sales_price to calculate sales_amount
    And sum ss_quantity to calculate quantity_sold
    And sum ss_net_profit to calculate net_profit
    And concatenate d_year and d_moy with hyphen separator to create year_month