Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data using a star schema
  So that I can track sales performance by item and store per month

  Background: Source Schema
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_category:STRING, i_class:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_company_name:STRING |

  Background: Target Schema
    Given the following target tables will be created:
      | table_name      | columns |
      | fact_sales      | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, sales_quantity:NUMBER, sales_amount:NUMBER, net_paid_amount:NUMBER |
      | dim_date        | date_key:NUMBER, date_value:DATE, year:NUMBER, month:NUMBER, month_sequence:NUMBER |
      | dim_item        | item_key:NUMBER, item_id:STRING, item_description:STRING, brand:STRING, category:STRING, class:STRING |
      | dim_store       | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, company_name:STRING |

  Scenario: Create date dimension
    Given source table date_dim
    When transforming to dim_date
    Then map d_date_sk to date_key
    And map d_date to date_value
    And map d_year to year
    And map d_moy to month
    And map d_month_seq to month_sequence

  Scenario: Create item dimension
    Given source table item
    When transforming to dim_item
    Then map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_item_desc to item_description
    And map i_brand to brand
    And map i_category to category  
    And map i_class to class

  Scenario: Create store dimension
    Given source table store
    When transforming to dim_store
    Then map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And map s_company_name to company_name

  Scenario: Create sales fact table
    Given source tables store_sales, date_dim, item, and store
    When joining store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And joining store_sales to item on ss_item_sk equals i_item_sk
    And joining store_sales to store on ss_store_sk equals s_store_sk
    When transforming to fact_sales
    Then map ss_sold_date_sk to date_key
    And map ss_item_sk to item_key
    And map ss_store_sk to store_key
    And map ss_quantity to sales_quantity
    And map ss_ext_sales_price to sales_amount
    And map ss_net_paid to net_paid_amount

  Scenario: Enable sales by item per month analysis
    Given fact_sales joined with dim_date and dim_item
    When grouping by item_key, item_description, year, and month
    Then aggregate sales_amount as total_sales
    And aggregate sales_quantity as total_quantity
    And aggregate net_paid_amount as total_net_paid

  Scenario: Enable sales by store per month analysis
    Given fact_sales joined with dim_date and dim_store
    When grouping by store_key, store_name, year, and month
    Then aggregate sales_amount as total_sales
    And aggregate sales_quantity as total_quantity
    And aggregate net_paid_amount as total_net_paid