Feature: sales_channel_summary

  Background: Schema specification for sales channel summary data product
    Given source table "store_sales" with columns:
      | column_name | data_type |
      | ss_sold_date_sk | NUMBER |
      | ss_item_sk | NUMBER |
      | ss_store_sk | NUMBER |
      | ss_quantity | NUMBER |
      | ss_sales_price | NUMBER |
      | ss_net_profit | NUMBER |
    
    And source table "catalog_sales" with columns:
      | column_name | data_type |
      | cs_sold_date_sk | NUMBER |
      | cs_item_sk | NUMBER |
      | cs_catalog_page_sk | NUMBER |
      | cs_quantity | NUMBER |
      | cs_sales_price | NUMBER |
      | cs_net_profit | NUMBER |
    
    And source table "web_sales" with columns:
      | column_name | data_type |
      | ws_sold_date_sk | NUMBER |
      | ws_item_sk | NUMBER |
      | ws_web_site_sk | NUMBER |
      | ws_quantity | NUMBER |
      | ws_sales_price | NUMBER |
      | ws_net_profit | NUMBER |
    
    And source table "item" with columns:
      | column_name | data_type |
      | i_item_sk | NUMBER |
      | i_item_id | STRING |
      | i_category | STRING |
      | i_brand | STRING |
      | i_product_name | STRING |
    
    And source table "store" with columns:
      | column_name | data_type |
      | s_store_sk | NUMBER |
      | s_store_id | STRING |
      | s_store_name | STRING |
      | s_state | STRING |
    
    And source table "catalog_page" with columns:
      | column_name | data_type |
      | cp_catalog_page_sk | NUMBER |
      | cp_catalog_page_id | STRING |
      | cp_department | STRING |
    
    And source table "date_dim" with columns:
      | column_name | data_type |
      | d_date_sk | NUMBER |
      | d_date | DATE |
      | d_year | NUMBER |
      | d_month_seq | NUMBER |
      | d_quarter_name | STRING |
    
    And target table "sales_channel_summary" with columns:
      | column_name | data_type |
      | sale_date | DATE |
      | year | NUMBER |
      | quarter | STRING |
      | channel_type | STRING |
      | channel_id | STRING |
      | channel_name | STRING |
      | item_category | STRING |
      | item_brand | STRING |
      | total_quantity | NUMBER |
      | total_sales_amount | NUMBER |
      | total_net_profit | NUMBER |
      | average_sale_price | NUMBER |
      | transaction_count | NUMBER |

  Scenario: Extract and transform store channel sales data
    Given data from "store_sales" table
    When joining "store_sales" with "date_dim" on ss_sold_date_sk equals d_date_sk
    And joining with "item" on ss_item_sk equals i_item_sk  
    And joining with "store" on ss_store_sk equals s_store_sk
    Then map channel_type as literal string "Store"
    And map channel_id from s_store_id
    And map channel_name from s_store_name
    And map sale_date from d_date
    And map year from d_year
    And map quarter from d_quarter_name
    And map item_category from i_category
    And map item_brand from i_brand
    And aggregate total_quantity by summing ss_quantity
    And aggregate total_sales_amount by summing ss_sales_price
    And aggregate total_net_profit by summing ss_net_profit
    And calculate average_sale_price by dividing total_sales_amount by total_quantity
    And calculate transaction_count by counting distinct transactions

  Scenario: Extract and transform catalog channel sales data
    Given data from "catalog_sales" table
    When joining "catalog_sales" with "date_dim" on cs_sold_date_sk equals d_date_sk
    And joining with "item" on cs_item_sk equals i_item_sk
    And joining with "catalog_page" on cs_catalog_page_sk equals cp_catalog_page_sk
    Then map channel_type as literal string "Catalog"
    And map channel_id from cp_catalog_page_id
    And map channel_name from cp_department
    And map sale_date from d_date
    And map year from d_year
    And map quarter from d_quarter_name
    And map item_category from i_category
    And map item_brand from i_brand
    And aggregate total_quantity by summing cs_quantity
    And aggregate total_sales_amount by summing cs_sales_price
    And aggregate total_net_profit by summing cs_net_profit
    And calculate average_sale_price by dividing total_sales_amount by total_quantity
    And calculate transaction_count by counting distinct transactions

  Scenario: Extract and transform web channel sales data
    Given data from "web_sales" table
    When joining "web_sales" with "date_dim" on ws_sold_date_sk equals d_date_sk
    And joining with "item" on ws_item_sk equals i_item_sk
    Then map channel_type as literal string "Web"
    And map channel_id from ws_web_site_sk converted to string
    And map channel_name as literal string "Online Store"
    And map sale_date from d_date
    And map year from d_year
    And map quarter from d_quarter_name
    And map item_category from i_category
    And map item_brand from i_brand
    And aggregate total_quantity by summing ws_quantity
    And aggregate total_sales_amount by summing ws_sales_price
    And aggregate total_net_profit by summing ws_net_profit
    And calculate average_sale_price by dividing total_sales_amount by total_quantity
    And calculate transaction_count by counting distinct transactions

  Scenario: Combine all channel data
    Given transformed store sales data
    And transformed catalog sales data  
    And transformed web sales data
    When combining all channel datasets using union operation
    Then group by sale_date, year, quarter, channel_type, channel_id, channel_name, item_category, item_brand
    And aggregate all numeric measures by summing values
    And recalculate average_sale_price as total_sales_amount divided by total_quantity