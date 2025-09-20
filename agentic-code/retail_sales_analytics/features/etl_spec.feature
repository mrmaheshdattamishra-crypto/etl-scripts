Feature: retail_sales_analytics
  As a business analyst
  I want to create a unified sales analytics data model
  So that I can analyze customer behavior and sales performance across all channels

  Background:
    Given the following source schema:
      | table_name     | column_name           | data_type    |
      | store_sales    | ss_sold_date_sk       | NUMBER       |
      | store_sales    | ss_item_sk            | NUMBER       |
      | store_sales    | ss_customer_sk        | NUMBER       |
      | store_sales    | ss_store_sk           | NUMBER       |
      | store_sales    | ss_quantity           | NUMBER       |
      | store_sales    | ss_sales_price        | NUMBER       |
      | store_sales    | ss_ext_sales_price    | NUMBER       |
      | store_sales    | ss_net_profit         | NUMBER       |
      | web_sales      | ws_sold_date_sk       | NUMBER       |
      | web_sales      | ws_item_sk            | NUMBER       |
      | web_sales      | ws_bill_customer_sk   | NUMBER       |
      | web_sales      | ws_web_site_sk        | NUMBER       |
      | web_sales      | ws_quantity           | NUMBER       |
      | web_sales      | ws_sales_price        | NUMBER       |
      | web_sales      | ws_ext_sales_price    | NUMBER       |
      | web_sales      | ws_net_profit         | NUMBER       |
      | date_dim       | d_date_sk             | NUMBER       |
      | date_dim       | d_date                | DATE         |
      | date_dim       | d_year                | NUMBER       |
      | date_dim       | d_moy                 | NUMBER       |
      | date_dim       | d_day_name            | STRING       |
      | item           | i_item_sk             | NUMBER       |
      | item           | i_item_id             | STRING       |
      | item           | i_brand               | STRING       |
      | item           | i_category            | STRING       |
      | item           | i_product_name        | STRING       |
    
    And the following target schema:
      | table_name           | column_name        | data_type |
      | unified_sales_fact   | sale_id            | STRING    |
      | unified_sales_fact   | sale_date          | DATE      |
      | unified_sales_fact   | sale_year          | NUMBER    |
      | unified_sales_fact   | sale_month         | NUMBER    |
      | unified_sales_fact   | day_of_week        | STRING    |
      | unified_sales_fact   | customer_key       | NUMBER    |
      | unified_sales_fact   | item_key           | NUMBER    |
      | unified_sales_fact   | item_id            | STRING    |
      | unified_sales_fact   | product_name       | STRING    |
      | unified_sales_fact   | brand              | STRING    |
      | unified_sales_fact   | category           | STRING    |
      | unified_sales_fact   | channel            | STRING    |
      | unified_sales_fact   | store_key          | NUMBER    |
      | unified_sales_fact   | website_key        | NUMBER    |
      | unified_sales_fact   | quantity_sold      | NUMBER    |
      | unified_sales_fact   | unit_price         | NUMBER    |
      | unified_sales_fact   | total_sales_amount | NUMBER    |
      | unified_sales_fact   | net_profit         | NUMBER    |

  Scenario: Create unified sales fact table from store sales
    Given I have store_sales as source table
    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I join store_sales with item on ss_item_sk equals i_item_sk
    Then I should map the following fields:
      | source_field          | target_field       | transformation_logic                                    |
      | ss_ticket_number      | sale_id           | concatenate store channel prefix with ticket number     |
      | d_date                | sale_date         | use date dimension date field                           |
      | d_year                | sale_year         | extract year from date dimension                        |
      | d_moy                 | sale_month        | extract month from date dimension                       |
      | d_day_name            | day_of_week       | use day name from date dimension                        |
      | ss_customer_sk        | customer_key      | use store sales customer surrogate key                  |
      | ss_item_sk            | item_key          | use store sales item surrogate key                      |
      | i_item_id             | item_id           | use item master item identifier                         |
      | i_product_name        | product_name      | use item master product name                            |
      | i_brand               | brand             | use item master brand name                              |
      | i_category            | category          | use item master category                                |
      | literal_value         | channel           | set constant value as store for all store sales        |
      | ss_store_sk           | store_key         | use store sales store surrogate key                     |
      | null                  | website_key       | set as null for store channel sales                     |
      | ss_quantity           | quantity_sold     | use store sales quantity field                          |
      | ss_sales_price        | unit_price        | use store sales unit price field                        |
      | ss_ext_sales_price    | total_sales_amount| use store sales extended sales price                    |
      | ss_net_profit         | net_profit        | use store sales net profit field                        |

  Scenario: Create unified sales fact table from web sales
    Given I have web_sales as source table
    When I join web_sales with date_dim on ws_sold_date_sk equals d_date_sk
    And I join web_sales with item on ws_item_sk equals i_item_sk
    Then I should map the following fields:
      | source_field          | target_field       | transformation_logic                                    |
      | ws_order_number       | sale_id           | concatenate web channel prefix with order number        |
      | d_date                | sale_date         | use date dimension date field                           |
      | d_year                | sale_year         | extract year from date dimension                        |
      | d_moy                 | sale_month        | extract month from date dimension                       |
      | d_day_name            | day_of_week       | use day name from date dimension                        |
      | ws_bill_customer_sk   | customer_key      | use web sales billing customer surrogate key           |
      | ws_item_sk            | item_key          | use web sales item surrogate key                        |
      | i_item_id             | item_id           | use item master item identifier                         |
      | i_product_name        | product_name      | use item master product name                            |
      | i_brand               | brand             | use item master brand name                              |
      | i_category            | category          | use item master category                                |
      | literal_value         | channel           | set constant value as web for all web sales             |
      | null                  | store_key         | set as null for web channel sales                       |
      | ws_web_site_sk        | website_key       | use web sales website surrogate key                     |
      | ws_quantity           | quantity_sold     | use web sales quantity field                            |
      | ws_sales_price        | unit_price        | use web sales unit price field                          |
      | ws_ext_sales_price    | total_sales_amount| use web sales extended sales price                      |
      | ws_net_profit         | net_profit        | use web sales net profit field                          |

  Scenario: Union all sales channels into unified fact table
    Given I have transformed store sales data
    And I have transformed web sales data
    When I union all channel data together
    Then I should create unified_sales_fact table with all sales transactions across channels
    And I should ensure data quality by removing null customer keys
    And I should ensure data quality by removing null item keys
    And I should ensure data quality by removing negative quantities