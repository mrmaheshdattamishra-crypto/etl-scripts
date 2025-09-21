Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track KPIs for sales by item/month and sales by store/month

  Background:
    Given source schema with tables:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_market_desc:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER |
    
    And target schema with tables:
      | table_name           | columns |
      | fact_monthly_sales   | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, year_month:STRING, total_sales_amount:NUMBER, total_quantity:NUMBER, total_net_paid:NUMBER |
      | dim_item             | item_key:NUMBER, item_id:STRING, item_description:STRING, brand:STRING, class:STRING, category:STRING, product_name:STRING |
      | dim_store            | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, market_description:STRING |
      | dim_date_month       | date_key:NUMBER, year_month:STRING, year:NUMBER, month:NUMBER, month_sequence:NUMBER |

  Scenario: Create dimension table for items
    Given source table "item"
    When transforming to target table "dim_item"
    Then map "i_item_sk" to "item_key"
    And map "i_item_id" to "item_id"
    And map "i_item_desc" to "item_description"
    And map "i_brand" to "brand"
    And map "i_class" to "class"
    And map "i_category" to "category"
    And map "i_product_name" to "product_name"

  Scenario: Create dimension table for stores
    Given source table "store"
    When transforming to target table "dim_store"
    Then map "s_store_sk" to "store_key"
    And map "s_store_id" to "store_id"
    And map "s_store_name" to "store_name"
    And map "s_city" to "city"
    And map "s_state" to "state"
    And map "s_market_desc" to "market_description"

  Scenario: Create dimension table for monthly dates
    Given source table "date_dim"
    When transforming to target table "dim_date_month"
    Then map "d_date_sk" to "date_key"
    And create "year_month" by concatenating year and month with hyphen from "d_year" and "d_moy"
    And map "d_year" to "year"
    And map "d_moy" to "month"
    And map "d_month_seq" to "month_sequence"

  Scenario: Create fact table for monthly sales
    Given source tables "store_sales", "date_dim"
    When joining "store_sales" with "date_dim" on "ss_sold_date_sk" equals "d_date_sk"
    And transforming to target table "fact_monthly_sales"
    Then map "d_date_sk" to "date_key"
    And map "ss_item_sk" to "item_key"
    And map "ss_store_sk" to "store_key"
    And create "year_month" by concatenating year and month with hyphen from "d_year" and "d_moy"
    And aggregate "ss_ext_sales_price" by sum and group by date_key, item_key, store_key, year_month to create "total_sales_amount"
    And aggregate "ss_quantity" by sum and group by date_key, item_key, store_key, year_month to create "total_quantity"
    And aggregate "ss_net_paid" by sum and group by date_key, item_key, store_key, year_month to create "total_net_paid"