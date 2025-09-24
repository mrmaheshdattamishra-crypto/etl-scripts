Feature: sales_summary_by_channel
  As a data analyst
  I want to create a sales summary dataset
  So that I can analyze sale patterns across different channels

  Background: Schema Specification
    Given source tables with the following schema:
      | table_name    | column_name           | data_type    |
      | store_sales   | ss_sold_date_sk      | NUMBER       |
      | store_sales   | ss_item_sk           | NUMBER       |
      | store_sales   | ss_store_sk          | NUMBER       |
      | store_sales   | ss_quantity          | NUMBER       |
      | store_sales   | ss_ext_sales_price   | NUMBER       |
      | store_sales   | ss_net_profit        | NUMBER       |
      | catalog_sales | cs_sold_date_sk      | NUMBER       |
      | catalog_sales | cs_item_sk           | NUMBER       |
      | catalog_sales | cs_quantity          | NUMBER       |
      | catalog_sales | cs_ext_sales_price   | NUMBER       |
      | catalog_sales | cs_net_profit        | NUMBER       |
      | web_sales     | ws_sold_date_sk      | NUMBER       |
      | web_sales     | ws_item_sk           | NUMBER       |
      | web_sales     | ws_quantity          | NUMBER       |
      | web_sales     | ws_ext_sales_price   | NUMBER       |
      | web_sales     | ws_net_profit        | NUMBER       |
      | date_dim      | d_date_sk            | NUMBER       |
      | date_dim      | d_date               | DATE         |
      | date_dim      | d_year               | NUMBER       |
      | date_dim      | d_moy                | NUMBER       |
      | date_dim      | d_quarter_name       | STRING       |
      | item          | i_item_sk            | NUMBER       |
      | item          | i_category           | STRING       |
      | item          | i_brand              | STRING       |
      | store         | s_store_sk           | NUMBER       |
      | store         | s_store_name         | STRING       |
      | store         | s_state              | STRING       |
    
    And target table with the following schema:
      | table_name           | column_name          | data_type    |
      | sales_summary        | sale_date            | DATE         |
      | sales_summary        | year                 | NUMBER       |
      | sales_summary        | month                | NUMBER       |
      | sales_summary        | quarter              | STRING       |
      | sales_summary        | channel              | STRING       |
      | sales_summary        | store_name           | STRING       |
      | sales_summary        | store_state          | STRING       |
      | sales_summary        | item_category        | STRING       |
      | sales_summary        | item_brand           | STRING       |
      | sales_summary        | total_quantity       | NUMBER       |
      | sales_summary        | total_sales_amount   | NUMBER       |
      | sales_summary        | total_profit         | NUMBER       |
      | sales_summary        | transaction_count    | NUMBER       |

  Scenario: Join Relations
    Given I need to join tables to create comprehensive sales summary
    When I combine sales data from multiple channels
    Then I should join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And I should join store_sales to item on ss_item_sk equals i_item_sk
    And I should join store_sales to store on ss_store_sk equals s_store_sk
    And I should join catalog_sales to date_dim on cs_sold_date_sk equals d_date_sk
    And I should join catalog_sales to item on cs_item_sk equals i_item_sk
    And I should join web_sales to date_dim on ws_sold_date_sk equals d_date_sk
    And I should join web_sales to item on ws_item_sk equals i_item_sk

  Scenario: Transform store sales data
    Given store sales transactions
    When I process store channel data
    Then map d_date to sale_date
    And map d_year to year
    And map d_moy to month
    And map d_quarter_name to quarter
    And set channel to store
    And map s_store_name to store_name
    And map s_state to store_state
    And map i_category to item_category
    And map i_brand to item_brand
    And sum ss_quantity to total_quantity
    And sum ss_ext_sales_price to total_sales_amount
    And sum ss_net_profit to total_profit
    And count distinct ss_ticket_number to transaction_count

  Scenario: Transform catalog sales data
    Given catalog sales transactions
    When I process catalog channel data
    Then map d_date to sale_date
    And map d_year to year
    And map d_moy to month
    And map d_quarter_name to quarter
    And set channel to catalog
    And set store_name to null
    And set store_state to null
    And map i_category to item_category
    And map i_brand to item_brand
    And sum cs_quantity to total_quantity
    And sum cs_ext_sales_price to total_sales_amount
    And sum cs_net_profit to total_profit
    And count distinct cs_order_number to transaction_count

  Scenario: Transform web sales data
    Given web sales transactions
    When I process web channel data
    Then map d_date to sale_date
    And map d_year to year
    And map d_moy to month
    And map d_quarter_name to quarter
    And set channel to web
    And set store_name to null
    And set store_state to null
    And map i_category to item_category
    And map i_brand to item_brand
    And sum ws_quantity to total_quantity
    And sum ws_ext_sales_price to total_sales_amount
    And sum ws_net_profit to total_profit
    And count distinct ws_order_number to transaction_count

  Scenario: Aggregate sales summary across channels
    Given transformed sales data from all channels
    When I create the final sales summary
    Then group by sale_date, year, month, quarter, channel, store_name, store_state, item_category, and item_brand
    And sum total_quantity across all groups
    And sum total_sales_amount across all groups
    And sum total_profit across all groups
    And sum transaction_count across all groups