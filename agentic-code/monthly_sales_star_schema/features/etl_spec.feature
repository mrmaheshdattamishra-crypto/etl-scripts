Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance by item and store
  So that I can track KPIs for sales by item/month and sales by store/month

Background:
  Given the following source tables:
    | Table Name  | Columns |
    | store_sales | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_paid (NUMBER), ss_net_profit (NUMBER) |
    | item        | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_class (STRING), i_category (STRING), i_product_name (STRING) |
    | store       | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_division_name (STRING), s_company_name (STRING) |
    | date_dim    | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_month_seq (NUMBER) |

  And the following target star schema:
    | Table Name     | Columns |
    | fact_monthly_sales | date_key (NUMBER), item_key (NUMBER), store_key (NUMBER), sales_quantity (NUMBER), sales_amount (NUMBER), net_sales (NUMBER), profit_amount (NUMBER) |
    | dim_item       | item_key (NUMBER), item_id (STRING), item_description (STRING), brand_name (STRING), item_class (STRING), category_name (STRING), product_name (STRING) |
    | dim_store      | store_key (NUMBER), store_id (STRING), store_name (STRING), city_name (STRING), state_name (STRING), division_name (STRING), company_name (STRING) |
    | dim_date       | date_key (NUMBER), calendar_date (DATE), year_number (NUMBER), month_number (NUMBER), month_sequence (NUMBER) |

Scenario: Load item dimension table
  Given source table item
  When extracting item dimension data
  Then populate dim_item with item_key as i_item_sk
  And item_id as i_item_id
  And item_description as i_item_desc
  And brand_name as i_brand
  And item_class as i_class
  And category_name as i_category
  And product_name as i_product_name

Scenario: Load store dimension table
  Given source table store
  When extracting store dimension data
  Then populate dim_store with store_key as s_store_sk
  And store_id as s_store_id
  And store_name as s_store_name
  And city_name as s_city
  And state_name as s_state
  And division_name as s_division_name
  And company_name as s_company_name

Scenario: Load date dimension table
  Given source table date_dim
  When extracting date dimension data
  Then populate dim_date with date_key as d_date_sk
  And calendar_date as d_date
  And year_number as d_year
  And month_number as d_moy
  And month_sequence as d_month_seq

Scenario: Load monthly sales fact table
  Given source table store_sales as main fact table
  When joining store_sales with date_dim on ss_sold_date_sk equals d_date_sk
  And joining with item on ss_item_sk equals i_item_sk
  And joining with store on ss_store_sk equals s_store_sk
  Then populate fact_monthly_sales with date_key as ss_sold_date_sk
  And item_key as ss_item_sk
  And store_key as ss_store_sk
  And sales_quantity as sum of ss_quantity grouped by month year item and store
  And sales_amount as sum of ss_sales_price grouped by month year item and store
  And net_sales as sum of ss_net_paid grouped by month year item and store
  And profit_amount as sum of ss_net_profit grouped by month year item and store
  And group data by d_year and d_moy and ss_item_sk and ss_store_sk