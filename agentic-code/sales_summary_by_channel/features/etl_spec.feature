Feature: sales_summary_by_channel

Scenario: Source Schema Definition
  Given source table "store_sales" with schema:
    | Column Name           | Data Type    |
    | ss_sold_date_sk      | NUMBER       |
    | ss_item_sk           | NUMBER       |
    | ss_quantity          | NUMBER       |
    | ss_sales_price       | NUMBER       |
    | ss_ext_sales_price   | NUMBER       |
    | ss_net_profit        | NUMBER       |
    | ss_store_sk          | NUMBER       |

  And source table "catalog_sales" with schema:
    | Column Name           | Data Type    |
    | cs_sold_date_sk      | NUMBER       |
    | cs_item_sk           | NUMBER       |
    | cs_quantity          | NUMBER       |
    | cs_sales_price       | NUMBER       |
    | cs_ext_sales_price   | NUMBER       |
    | cs_net_profit        | NUMBER       |
    | cs_call_center_sk    | NUMBER       |

  And source table "web_sales" with schema:
    | Column Name           | Data Type    |
    | ws_sold_date_sk      | NUMBER       |
    | ws_item_sk           | NUMBER       |
    | ws_quantity          | NUMBER       |
    | ws_sales_price       | NUMBER       |
    | ws_ext_sales_price   | NUMBER       |
    | ws_net_profit        | NUMBER       |
    | ws_web_site_sk       | NUMBER       |

  And source table "date_dim" with schema:
    | Column Name    | Data Type |
    | d_date_sk     | NUMBER    |
    | d_date        | DATE      |
    | d_year        | NUMBER    |
    | d_moy         | NUMBER    |
    | d_qoy         | NUMBER    |
    | d_day_name    | STRING    |

  And source table "item" with schema:
    | Column Name    | Data Type |
    | i_item_sk     | NUMBER    |
    | i_item_id     | STRING    |
    | i_category    | STRING    |
    | i_brand       | STRING    |
    | i_class       | STRING    |

Scenario: Target Schema Definition
  Given target table "sales_summary_by_channel" with schema:
    | Column Name           | Data Type |
    | sale_date            | DATE      |
    | year                 | NUMBER    |
    | month                | NUMBER    |
    | quarter              | NUMBER    |
    | day_name             | STRING    |
    | channel              | STRING    |
    | item_category        | STRING    |
    | item_brand           | STRING    |
    | item_class           | STRING    |
    | total_quantity       | NUMBER    |
    | total_sales_amount   | NUMBER    |
    | total_net_profit     | NUMBER    |
    | avg_unit_price       | NUMBER    |
    | transaction_count    | NUMBER    |

Scenario: Join Relations
  Given store_sales joins with date_dim on store_sales.ss_sold_date_sk equals date_dim.d_date_sk
  And store_sales joins with item on store_sales.ss_item_sk equals item.i_item_sk
  And catalog_sales joins with date_dim on catalog_sales.cs_sold_date_sk equals date_dim.d_date_sk
  And catalog_sales joins with item on catalog_sales.cs_item_sk equals item.i_item_sk
  And web_sales joins with date_dim on web_sales.ws_sold_date_sk equals date_dim.d_date_sk
  And web_sales joins with item on web_sales.ws_item_sk equals item.i_item_sk

Scenario: Data Mapping
  Given I want to create a unified sales summary across all channels
  When I combine store sales, catalog sales, and web sales data
  Then I should map sale_date from the date dimension table date field
  And I should map year from the date dimension table year field
  And I should map month from the date dimension table month field
  And I should map quarter from the date dimension table quarter field
  And I should map day_name from the date dimension table day name field
  And I should map channel as store for store sales records
  And I should map channel as catalog for catalog sales records
  And I should map channel as web for web sales records
  And I should map item_category from the item table category field
  And I should map item_brand from the item table brand field
  And I should map item_class from the item table class field
  And I should sum quantity across all channels to get total_quantity
  And I should sum extended sales price across all channels to get total_sales_amount
  And I should sum net profit across all channels to get total_net_profit
  And I should calculate average unit price as total sales amount divided by total quantity
  And I should count distinct transactions to get transaction_count
  And I should group by sale date, channel, item category, item brand, and item class