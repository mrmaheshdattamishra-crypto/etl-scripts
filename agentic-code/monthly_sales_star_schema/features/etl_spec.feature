Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background:
    Given source tables:
      | table_name  | columns                                                                           |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_ext_sales_price:NUMBER, ss_quantity:NUMBER, ss_net_paid:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_division_name:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER, d_quarter_name:STRING |

    And target star schema:
      | table_name    | table_type | columns                                                                         |
      | fact_monthly_sales | fact  | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, year_month:STRING, total_sales:NUMBER, total_quantity:NUMBER, total_net_paid:NUMBER |
      | dim_item      | dimension | item_key:NUMBER, item_id:STRING, item_desc:STRING, brand:STRING, category:STRING, product_name:STRING |
      | dim_store     | dimension | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, division_name:STRING |
      | dim_date      | dimension | date_key:NUMBER, date:DATE, year:NUMBER, month:NUMBER, month_seq:NUMBER, quarter_name:STRING, year_month:STRING |

  Scenario: Build item dimension table
    Given source table item
    When transforming to dim_item
    Then map item_key from i_item_sk
    And map item_id from i_item_id
    And map item_desc from i_item_desc  
    And map brand from i_brand
    And map category from i_category
    And map product_name from i_product_name

  Scenario: Build store dimension table
    Given source table store
    When transforming to dim_store
    Then map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state
    And map division_name from s_division_name

  Scenario: Build date dimension table
    Given source table date_dim
    When transforming to dim_date
    Then map date_key from d_date_sk
    And map date from d_date
    And map year from d_year
    And map month from d_moy
    And map month_seq from d_month_seq
    And map quarter_name from d_quarter_name
    And map year_month by concatenating year and month with hyphen separator

  Scenario: Build monthly sales fact table
    Given source tables store_sales, date_dim
    When joining store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And transforming to fact_monthly_sales
    Then map date_key from d_date_sk
    And map item_key from ss_item_sk
    And map store_key from ss_store_sk
    And map year_month by concatenating d_year and d_moy with hyphen separator
    And map total_sales by summing ss_ext_sales_price grouped by item_key, store_key, and year_month
    And map total_quantity by summing ss_quantity grouped by item_key, store_key, and year_month
    And map total_net_paid by summing ss_net_paid grouped by item_key, store_key, and year_month