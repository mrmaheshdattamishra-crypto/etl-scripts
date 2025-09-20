Feature: monthly_sales_analysis

Background: Star schema for analyzing monthly sales with KPIs for sales by item/month and sales by store/month

  Given the following source tables:
    | Table Name | Columns |
    | store_sales | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_store_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_paid (NUMBER) |
    | date_dim | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_moy (NUMBER), d_month_seq (NUMBER) |
    | item | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_category (STRING), i_product_name (STRING) |
    | store | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING) |

  And the following target data model:
    | Table Name | Columns |
    | fact_monthly_sales | date_key (NUMBER), item_key (NUMBER), store_key (NUMBER), sales_month (NUMBER), sales_year (NUMBER), total_quantity (NUMBER), total_sales_amount (NUMBER), total_net_paid (NUMBER) |
    | dim_date | date_key (NUMBER), full_date (DATE), year (NUMBER), month (NUMBER), month_name (STRING), quarter (NUMBER) |
    | dim_item | item_key (NUMBER), item_id (STRING), item_description (STRING), brand (STRING), category (STRING), product_name (STRING) |
    | dim_store | store_key (NUMBER), store_id (STRING), store_name (STRING), city (STRING), state (STRING) |

Scenario: Create dimension tables
  When loading dim_date
  Then extract date_key from d_date_sk
  And extract full_date from d_date
  And extract year from d_year
  And extract month from d_moy
  And derive month_name by converting month number to month name
  And derive quarter by dividing month by three and rounding up

  When loading dim_item
  Then extract item_key from i_item_sk
  And extract item_id from i_item_id
  And extract item_description from i_item_desc
  And extract brand from i_brand
  And extract category from i_category
  And extract product_name from i_product_name

  When loading dim_store
  Then extract store_key from s_store_sk
  And extract store_id from s_store_id
  And extract store_name from s_store_name
  And extract city from s_city
  And extract state from s_state

Scenario: Create fact table for monthly sales analysis
  When loading fact_monthly_sales
  Then join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
  And join with item on ss_item_sk equals i_item_sk
  And join with store on ss_store_sk equals s_store_sk
  And extract date_key from d_date_sk
  And extract item_key from i_item_sk
  And extract store_key from s_store_sk
  And extract sales_month from d_moy
  And extract sales_year from d_year
  And aggregate total_quantity by summing ss_quantity grouped by date_key, item_key, store_key, sales_month, sales_year
  And aggregate total_sales_amount by summing ss_ext_sales_price grouped by date_key, item_key, store_key, sales_month, sales_year
  And aggregate total_net_paid by summing ss_net_paid grouped by date_key, item_key, store_key, sales_month, sales_year