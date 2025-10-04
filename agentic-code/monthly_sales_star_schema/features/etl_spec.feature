# Reference: Similar ticket SCRUM-9 with identical requirements for monthly sales star schema

Feature: monthly_sales_star_schema
  As a business analyst
  I want a star schema for monthly sales analysis
  So that I can analyze sales by item/month and sales by store/month

  Background:
    Given source schema with tables:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_ext_sales_price:NUMBER, ss_quantity:NUMBER, ss_net_profit:NUMBER |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_date_id:STRING |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING |

    And target schema with tables:
      | table_name     | columns |
      | fact_monthly_sales | sale_month:STRING, item_key:NUMBER, store_key:NUMBER, total_sales_amount:NUMBER, total_quantity:NUMBER, total_profit:NUMBER, transaction_count:NUMBER |
      | dim_item       | item_key:NUMBER, item_id:STRING, item_name:STRING, item_description:STRING, brand:STRING, category:STRING |
      | dim_store      | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING |
      | dim_month      | sale_month:STRING, year:NUMBER, month_number:NUMBER, month_name:STRING |

  Scenario: Create item dimension
    Given source table item
    When transforming to dim_item
    Then map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_product_name to item_name
    And map i_item_desc to item_description
    And map i_brand to brand
    And map i_category to category

  Scenario: Create store dimension
    Given source table store
    When transforming to dim_store
    Then map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state

  Scenario: Create month dimension
    Given source table date_dim
    When transforming to dim_month
    Then create sale_month by concatenating d_year and d_moy with hyphen separator
    And map d_year to year
    And map d_moy to month_number
    And create month_name by converting month_number to month name

  Scenario: Create monthly sales fact table
    Given source tables store_sales, date_dim, item, store
    When joining store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And joining store_sales to item on ss_item_sk equals i_item_sk
    And joining store_sales to store on ss_store_sk equals s_store_sk
    And transforming to fact_monthly_sales
    Then create sale_month by concatenating d_year and d_moy with hyphen separator
    And map ss_item_sk to item_key
    And map ss_store_sk to store_key
    And aggregate ss_ext_sales_price by sum as total_sales_amount grouped by sale_month, item_key, store_key
    And aggregate ss_quantity by sum as total_quantity grouped by sale_month, item_key, store_key
    And aggregate ss_net_profit by sum as total_profit grouped by sale_month, item_key, store_key
    And count distinct ss_ticket_number as transaction_count grouped by sale_month, item_key, store_key