Feature: sales_summary_by_channel
  As a business analyst
  I want to create a sales summary dataset
  So that I can analyze sale patterns across different channels

  Background:
    Given the following source tables:
      | table_name    | columns |
      | store_sales   | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER, ss_net_profit:NUMBER |
      | web_sales     | ws_sold_date_sk:NUMBER, ws_item_sk:NUMBER, ws_quantity:NUMBER, ws_sales_price:NUMBER, ws_ext_sales_price:NUMBER, ws_net_paid:NUMBER, ws_net_profit:NUMBER |
      | catalog_sales | cs_sold_date_sk:NUMBER, cs_item_sk:NUMBER, cs_quantity:NUMBER, cs_sales_price:NUMBER, cs_ext_sales_price:NUMBER, cs_net_paid:NUMBER, cs_net_profit:NUMBER |
      | date_dim      | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_qoy:NUMBER, d_day_name:STRING, d_quarter_name:STRING |
      | item          | i_item_sk:NUMBER, i_item_id:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |

    And the target table schema:
      | table_name              | columns |
      | sales_summary_by_channel | channel:STRING, sale_date:DATE, year:NUMBER, month:NUMBER, quarter:NUMBER, item_id:STRING, brand:STRING, category:STRING, product_name:STRING, total_quantity:NUMBER, total_sales:NUMBER, total_net_paid:NUMBER, total_net_profit:NUMBER, transaction_count:NUMBER |

  Scenario: Extract and transform store sales data
    Given store sales records from store_sales table
    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I join the result with item on ss_item_sk equals i_item_sk
    Then I transform the data by setting channel to 'Store'
    And I extract sale_date from d_date
    And I extract year from d_year
    And I extract month from d_moy
    And I extract quarter from d_qoy
    And I extract item_id from i_item_id
    And I extract brand from i_brand
    And I extract category from i_category
    And I extract product_name from i_product_name
    And I aggregate total_quantity by summing ss_quantity
    And I aggregate total_sales by summing ss_ext_sales_price
    And I aggregate total_net_paid by summing ss_net_paid
    And I aggregate total_net_profit by summing ss_net_profit
    And I count transaction_count as number of records
    And I group by channel, sale_date, year, month, quarter, item_id, brand, category, product_name

  Scenario: Extract and transform web sales data
    Given web sales records from web_sales table
    When I join web_sales with date_dim on ws_sold_date_sk equals d_date_sk
    And I join the result with item on ws_item_sk equals i_item_sk
    Then I transform the data by setting channel to 'Web'
    And I extract sale_date from d_date
    And I extract year from d_year
    And I extract month from d_moy
    And I extract quarter from d_qoy
    And I extract item_id from i_item_id
    And I extract brand from i_brand
    And I extract category from i_category
    And I extract product_name from i_product_name
    And I aggregate total_quantity by summing ws_quantity
    And I aggregate total_sales by summing ws_ext_sales_price
    And I aggregate total_net_paid by summing ws_net_paid
    And I aggregate total_net_profit by summing ws_net_profit
    And I count transaction_count as number of records
    And I group by channel, sale_date, year, month, quarter, item_id, brand, category, product_name

  Scenario: Extract and transform catalog sales data
    Given catalog sales records from catalog_sales table
    When I join catalog_sales with date_dim on cs_sold_date_sk equals d_date_sk
    And I join the result with item on cs_item_sk equals i_item_sk
    Then I transform the data by setting channel to 'Catalog'
    And I extract sale_date from d_date
    And I extract year from d_year
    And I extract month from d_moy
    And I extract quarter from d_qoy
    And I extract item_id from i_item_id
    And I extract brand from i_brand
    And I extract category from i_category
    And I extract product_name from i_product_name
    And I aggregate total_quantity by summing cs_quantity
    And I aggregate total_sales by summing cs_ext_sales_price
    And I aggregate total_net_paid by summing cs_net_paid
    And I aggregate total_net_profit by summing cs_net_profit
    And I count transaction_count as number of records
    And I group by channel, sale_date, year, month, quarter, item_id, brand, category, product_name

  Scenario: Combine all channel data
    Given transformed store sales data
    And transformed web sales data
    And transformed catalog sales data
    When I union all three datasets
    Then I load the combined data into sales_summary_by_channel table