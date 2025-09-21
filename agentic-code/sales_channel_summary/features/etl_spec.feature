Feature: sales_channel_summary
  As a business analyst
  I want to analyze sales patterns across different channels
  So that I can understand channel performance and customer behavior

  Background:
    Given the following source tables with their schemas:
      | table_name     | columns                                                                                                                           |
      | catalog_sales  | cs_sold_date_sk:NUMBER, cs_item_sk:NUMBER, cs_quantity:NUMBER, cs_sales_price:NUMBER, cs_ext_sales_price:NUMBER, cs_net_profit:NUMBER, cs_call_center_sk:NUMBER |
      | web_sales      | ws_sold_date_sk:NUMBER, ws_item_sk:NUMBER, ws_quantity:NUMBER, ws_sales_price:NUMBER, ws_ext_sales_price:NUMBER, ws_net_profit:NUMBER, ws_web_site_sk:NUMBER     |
      | store_sales    | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_profit:NUMBER, ss_store_sk:NUMBER       |
      | item           | i_item_sk:NUMBER, i_item_id:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING              |
      | date_dim       | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_qoy:NUMBER, d_day_name:STRING, d_month_name:STRING           |
      | store          | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_state:STRING, s_company_name:STRING                           |
      | web_site       | web_site_sk:NUMBER, web_site_id:STRING, web_name:STRING, web_company_name:STRING                                            |
      | call_center    | cc_call_center_sk:NUMBER, cc_call_center_id:STRING, cc_name:STRING, cc_company_name:STRING                                 |

    And the target table schema:
      | table_name           | columns                                                                                                                                                                                    |
      | sales_channel_summary| sale_date:DATE, channel_type:STRING, channel_name:STRING, channel_company:STRING, item_category:STRING, item_brand:STRING, total_quantity:NUMBER, total_sales:NUMBER, total_profit:NUMBER, avg_unit_price:NUMBER, transaction_count:NUMBER |

  Scenario: Extract catalog sales data
    Given catalog sales transactions
    When processing catalog sales records
    Then join catalog_sales with date_dim on cs_sold_date_sk equals d_date_sk
    And join with item on cs_item_sk equals i_item_sk
    And join with call_center on cs_call_center_sk equals cc_call_center_sk
    And map sale_date to d_date from date_dim
    And map channel_type to literal string 'catalog'
    And map channel_name to cc_name from call_center
    And map channel_company to cc_company_name from call_center
    And map item_category to i_category from item
    And map item_brand to i_brand from item
    And map total_quantity to sum of cs_quantity
    And map total_sales to sum of cs_ext_sales_price
    And map total_profit to sum of cs_net_profit
    And map avg_unit_price to average of cs_sales_price
    And map transaction_count to count of distinct cs_order_number

  Scenario: Extract web sales data
    Given web sales transactions
    When processing web sales records
    Then join web_sales with date_dim on ws_sold_date_sk equals d_date_sk
    And join with item on ws_item_sk equals i_item_sk
    And join with web_site on ws_web_site_sk equals web_site_sk
    And map sale_date to d_date from date_dim
    And map channel_type to literal string 'web'
    And map channel_name to web_name from web_site
    And map channel_company to web_company_name from web_site
    And map item_category to i_category from item
    And map item_brand to i_brand from item
    And map total_quantity to sum of ws_quantity
    And map total_sales to sum of ws_ext_sales_price
    And map total_profit to sum of ws_net_profit
    And map avg_unit_price to average of ws_sales_price
    And map transaction_count to count of distinct ws_order_number

  Scenario: Extract store sales data
    Given store sales transactions
    When processing store sales records
    Then join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And join with item on ss_item_sk equals i_item_sk
    And join with store on ss_store_sk equals s_store_sk
    And map sale_date to d_date from date_dim
    And map channel_type to literal string 'store'
    And map channel_name to s_store_name from store
    And map channel_company to s_company_name from store
    And map item_category to i_category from item
    And map item_brand to i_brand from item
    And map total_quantity to sum of ss_quantity
    And map total_sales to sum of ss_ext_sales_price
    And map total_profit to sum of ss_net_profit
    And map avg_unit_price to average of ss_sales_price
    And map transaction_count to count of distinct ss_ticket_number

  Scenario: Combine all channel data
    Given catalog sales summary, web sales summary, and store sales summary
    When creating final sales channel summary
    Then union all three channel datasets
    And group by sale_date, channel_type, channel_name, channel_company, item_category, item_brand
    And aggregate all numeric measures by summing total_quantity, total_sales, total_profit, transaction_count
    And calculate avg_unit_price as total_sales divided by total_quantity
    And filter out records where total_sales is null or zero
    And order by sale_date descending, total_sales descending