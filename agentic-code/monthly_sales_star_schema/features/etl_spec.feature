Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales by item and store
  So that I can track performance and make informed decisions

  Background: Source Schema
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_paid (NUMBER), ss_net_profit (NUMBER) |
      | date_dim    | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_month_seq (NUMBER) |
      | item        | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_class (STRING), i_category (STRING), i_product_name (STRING) |
      | store       | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_market_desc (STRING) |

  Background: Target Schema
    Given the target star schema contains:
      | table_name        | table_type | columns |
      | fact_monthly_sales | fact       | date_key (NUMBER), item_key (NUMBER), store_key (NUMBER), sales_month (NUMBER), sales_year (NUMBER), total_quantity (NUMBER), total_sales_amount (NUMBER), total_net_paid (NUMBER), total_net_profit (NUMBER), transaction_count (NUMBER) |
      | dim_date          | dimension  | date_key (NUMBER), date_value (DATE), year (NUMBER), month (NUMBER), month_name (STRING), quarter (NUMBER), month_seq (NUMBER) |
      | dim_item          | dimension  | item_key (NUMBER), item_id (STRING), item_description (STRING), brand (STRING), class (STRING), category (STRING), product_name (STRING) |
      | dim_store         | dimension  | store_key (NUMBER), store_id (STRING), store_name (STRING), city (STRING), state (STRING), market_description (STRING) |

  Scenario: Build Date Dimension
    Given source table date_dim
    When transforming to dim_date
    Then map d_date_sk to date_key
    And map d_date to date_value
    And map d_year to year
    And map d_moy to month
    And derive month_name from d_moy using month number to name conversion
    And derive quarter from d_moy by dividing by three and rounding up
    And map d_month_seq to month_seq
    And filter records where d_date is not null

  Scenario: Build Item Dimension  
    Given source table item
    When transforming to dim_item
    Then map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_item_desc to item_description
    And map i_brand to brand
    And map i_class to class
    And map i_category to category
    And map i_product_name to product_name
    And filter records where i_item_sk is not null

  Scenario: Build Store Dimension
    Given source table store
    When transforming to dim_store  
    Then map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And map s_market_desc to market_description
    And filter records where s_store_sk is not null

  Scenario: Build Monthly Sales Fact Table
    Given source tables store_sales, date_dim, item, store
    When joining store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And joining store_sales to item on ss_item_sk equals i_item_sk
    And joining store_sales to store on ss_store_sk equals s_store_sk
    And grouping by d_date_sk, i_item_sk, s_store_sk, d_year, d_moy
    Then map d_date_sk to date_key
    And map i_item_sk to item_key
    And map s_store_sk to store_key
    And map d_moy to sales_month
    And map d_year to sales_year
    And aggregate ss_quantity using sum function to total_quantity
    And aggregate ss_ext_sales_price using sum function to total_sales_amount
    And aggregate ss_net_paid using sum function to total_net_paid
    And aggregate ss_net_profit using sum function to total_net_profit
    And count distinct ss_ticket_number to transaction_count
    And filter records where ss_sold_date_sk is not null
    And filter records where ss_item_sk is not null
    And filter records where ss_store_sk is not null