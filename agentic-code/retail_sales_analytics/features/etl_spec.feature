Feature: retail_sales_analytics
  As a data analyst
  I want to create a comprehensive retail sales analytics data product
  So that I can analyze sales performance across all channels

  Background:
    Given the following source tables are available:
      | table_name        | columns                                                                                                                     |
      | store_sales       | ss_sold_date_sk:NUMBER, ss_sold_time_sk:NUMBER, ss_item_sk:NUMBER, ss_customer_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_profit:NUMBER |
      | catalog_sales     | cs_sold_date_sk:NUMBER, cs_item_sk:NUMBER, cs_bill_customer_sk:NUMBER, cs_quantity:NUMBER, cs_sales_price:NUMBER, cs_ext_sales_price:NUMBER, cs_net_profit:NUMBER |
      | web_sales         | ws_sold_date_sk:NUMBER, ws_item_sk:NUMBER, ws_bill_customer_sk:NUMBER, ws_quantity:NUMBER, ws_sales_price:NUMBER, ws_ext_sales_price:NUMBER, ws_net_profit:NUMBER |
      | store             | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_market_desc:STRING |
      | customer          | c_customer_sk:NUMBER, c_customer_id:STRING, c_first_name:STRING, c_last_name:STRING, c_birth_year:NUMBER |
      | item              | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_category:STRING, i_brand:STRING |
      | date_dim          | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_month_seq:NUMBER, d_quarter_seq:NUMBER |
      | customer_address  | ca_address_sk:NUMBER, ca_city:STRING, ca_state:STRING, ca_country:STRING |

    And the target data model should be:
      | table_name                  | columns                                                                                                           |
      | unified_sales_fact          | sale_id:STRING, sale_date:DATE, channel:STRING, customer_key:NUMBER, item_key:NUMBER, store_key:NUMBER, quantity:NUMBER, unit_price:NUMBER, total_sales:NUMBER, profit:NUMBER |
      | customer_dimension          | customer_key:NUMBER, customer_id:STRING, full_name:STRING, birth_year:NUMBER, address_city:STRING, address_state:STRING |
      | item_dimension              | item_key:NUMBER, item_id:STRING, item_description:STRING, category:STRING, brand:STRING |
      | store_dimension             | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, market_description:STRING |
      | date_dimension              | date_key:NUMBER, calendar_date:DATE, year:NUMBER, month:NUMBER, quarter:NUMBER |
      | sales_summary_by_channel    | channel:STRING, sale_date:DATE, total_quantity:NUMBER, total_sales:NUMBER, total_profit:NUMBER |

  Scenario: Create unified sales fact table
    Given I need to combine sales data from all channels
    When I process the source data
    Then I should create unified_sales_fact table by:
      - Union store_sales, catalog_sales, and web_sales tables
      - Generate unique sale_id by concatenating channel prefix with surrogate keys
      - Map channel as 'STORE' for store_sales, 'CATALOG' for catalog_sales, 'WEB' for web_sales
      - Join with date_dim on date surrogate keys to get sale_date
      - Map customer surrogate keys to customer_key
      - Map item surrogate keys to item_key
      - Map store surrogate keys to store_key for store sales, set to null for other channels
      - Extract quantity, unit price as sales_price, total sales as ext_sales_price, and profit as net_profit

  Scenario: Build customer dimension
    Given I need customer master data
    When I process customer and customer_address tables
    Then I should create customer_dimension table by:
      - Join customer table with customer_address on address surrogate key
      - Concatenate first_name and last_name to create full_name
      - Map customer surrogate key to customer_key
      - Extract birth_year from customer table
      - Extract city and state from customer_address table

  Scenario: Build item dimension
    Given I need item master data
    When I process item table
    Then I should create item_dimension table by:
      - Map item surrogate key to item_key
      - Extract item_id, item_desc as item_description, category, and brand
      - Handle null values by replacing with 'Unknown'

  Scenario: Build store dimension
    Given I need store master data
    When I process store table
    Then I should create store_dimension table by:
      - Map store surrogate key to store_key
      - Extract store_id, store_name, city, state, and market_desc as market_description
      - Filter only active stores where rec_end_date is null

  Scenario: Build date dimension
    Given I need date master data
    When I process date_dim table
    Then I should create date_dimension table by:
      - Map date surrogate key to date_key
      - Extract d_date as calendar_date, d_year as year
      - Calculate month from month_seq and quarter from quarter_seq
      - Include only dates within business range

  Scenario: Create sales summary by channel
    Given I need aggregated sales metrics by channel
    When I process unified_sales_fact table
    Then I should create sales_summary_by_channel table by:
      - Group by channel and sale_date
      - Sum quantity to get total_quantity
      - Sum total_sales to get total_sales
      - Sum profit to get total_profit
      - Order by sale_date and channel

  Join Relations:
    - store_sales joins with date_dim on ss_sold_date_sk = d_date_sk
    - store_sales joins with customer on ss_customer_sk = c_customer_sk
    - store_sales joins with item on ss_item_sk = i_item_sk
    - store_sales joins with store on ss_store_sk = s_store_sk
    - catalog_sales joins with date_dim on cs_sold_date_sk = d_date_sk
    - catalog_sales joins with customer on cs_bill_customer_sk = c_customer_sk
    - catalog_sales joins with item on cs_item_sk = i_item_sk
    - web_sales joins with date_dim on ws_sold_date_sk = d_date_sk
    - web_sales joins with customer on ws_bill_customer_sk = c_customer_sk
    - web_sales joins with item on ws_item_sk = i_item_sk
    - customer joins with customer_address on c_current_addr_sk = ca_address_sk