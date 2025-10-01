# Reference: Similar to SCRUM-9 (CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT)
# This specification extends the existing monthly sales analysis by adding customer country dimension

Feature: monthly_sales_with_country_dimension

  Background: Source and Target Schema Definition
    Given source table "store_sales" with schema:
      | column_name           | data_type |
      | ss_sold_date_sk       | NUMBER    |
      | ss_customer_sk        | NUMBER    |
      | ss_addr_sk            | NUMBER    |
      | ss_item_sk            | NUMBER    |
      | ss_store_sk           | NUMBER    |
      | ss_quantity           | NUMBER    |
      | ss_sales_price        | NUMBER    |
      | ss_ext_sales_price    | NUMBER    |
      | ss_net_paid           | NUMBER    |
      | ss_net_profit         | NUMBER    |
    
    And source table "catalog_sales" with schema:
      | column_name           | data_type |
      | cs_sold_date_sk       | NUMBER    |
      | cs_bill_customer_sk   | NUMBER    |
      | cs_bill_addr_sk       | NUMBER    |
      | cs_item_sk            | NUMBER    |
      | cs_quantity           | NUMBER    |
      | cs_sales_price        | NUMBER    |
      | cs_ext_sales_price    | NUMBER    |
      | cs_net_paid           | NUMBER    |
      | cs_net_profit         | NUMBER    |
    
    And source table "web_sales" with schema:
      | column_name           | data_type |
      | ws_sold_date_sk       | NUMBER    |
      | ws_bill_customer_sk   | NUMBER    |
      | ws_bill_addr_sk       | NUMBER    |
      | ws_item_sk            | NUMBER    |
      | ws_quantity           | NUMBER    |
      | ws_sales_price        | NUMBER    |
      | ws_ext_sales_price    | NUMBER    |
      | ws_net_paid           | NUMBER    |
      | ws_net_profit         | NUMBER    |
    
    And source table "customer" with schema:
      | column_name           | data_type |
      | c_customer_sk         | NUMBER    |
      | c_current_addr_sk     | NUMBER    |
      | c_customer_id         | STRING    |
      | c_first_name          | STRING    |
      | c_last_name           | STRING    |
    
    And source table "customer_address" with schema:
      | column_name           | data_type |
      | ca_address_sk         | NUMBER    |
      | ca_address_id         | STRING    |
      | ca_street_number      | STRING    |
      | ca_street_name        | STRING    |
      | ca_city               | STRING    |
      | ca_county             | STRING    |
      | ca_state              | STRING    |
      | ca_zip                | STRING    |
      | ca_country            | STRING    |
    
    And source table "date_dim" with schema:
      | column_name           | data_type |
      | d_date_sk             | NUMBER    |
      | d_date                | DATE      |
      | d_year                | NUMBER    |
      | d_moy                 | NUMBER    |
      | d_month_seq           | NUMBER    |
    
    And source table "item" with schema:
      | column_name           | data_type |
      | i_item_sk             | NUMBER    |
      | i_item_id             | STRING    |
      | i_item_desc           | STRING    |
      | i_brand               | STRING    |
      | i_category            | STRING    |
    
    And source table "store" with schema:
      | column_name           | data_type |
      | s_store_sk            | NUMBER    |
      | s_store_id            | STRING    |
      | s_store_name          | STRING    |
      | s_market_id           | NUMBER    |
    
    And target table "fact_monthly_sales_by_country" with schema:
      | column_name           | data_type |
      | sale_year             | NUMBER    |
      | sale_month            | NUMBER    |
      | month_seq             | NUMBER    |
      | customer_country      | STRING    |
      | item_sk               | NUMBER    |
      | store_sk              | NUMBER    |
      | total_quantity        | NUMBER    |
      | total_sales_amount    | NUMBER    |
      | total_net_paid        | NUMBER    |
      | total_net_profit      | NUMBER    |
      | transaction_count     | NUMBER    |

  Scenario: Build monthly sales fact table with customer country dimension
    Given I have sales data from store_sales, catalog_sales, and web_sales
    And I need to aggregate sales by year, month, customer country, item, and store
    
    When I union all sales channels
    And I join with customer table on customer surrogate key
    And I join with customer_address table on address surrogate key
    And I join with date_dim table on sold date surrogate key
    
    Then for target column "sale_year" map source column "d_year" from date_dim
    And for target column "sale_month" map source column "d_moy" from date_dim
    And for target column "month_seq" map source column "d_month_seq" from date_dim
    And for target column "customer_country" map source column "ca_country" from customer_address
    And for target column "item_sk" map source column item surrogate key from sales tables
    And for target column "store_sk" map source column store surrogate key from sales tables where channel is store sales otherwise null
    And for target column "total_quantity" calculate sum of quantity from all sales channels
    And for target column "total_sales_amount" calculate sum of extended sales price from all sales channels
    And for target column "total_net_paid" calculate sum of net paid amount from all sales channels
    And for target column "total_net_profit" calculate sum of net profit from all sales channels
    And for target column "transaction_count" calculate count of distinct transactions
    
    And group by sale_year, sale_month, month_seq, customer_country, item_sk, and store_sk

  Scenario: Define join relationships for sales data integration
    Given store_sales joins with customer on ss_customer_sk equals c_customer_sk
    And catalog_sales joins with customer on cs_bill_customer_sk equals c_customer_sk
    And web_sales joins with customer on ws_bill_customer_sk equals c_customer_sk
    
    And store_sales joins with customer_address on ss_addr_sk equals ca_address_sk
    And catalog_sales joins with customer_address on cs_bill_addr_sk equals ca_address_sk
    And web_sales joins with customer_address on ws_bill_addr_sk equals ca_address_sk
    
    And store_sales joins with date_dim on ss_sold_date_sk equals d_date_sk
    And catalog_sales joins with date_dim on cs_sold_date_sk equals d_date_sk
    And web_sales joins with date_dim on ws_sold_date_sk equals d_date_sk
    
    And all sales tables join with item on item surrogate key equals i_item_sk
    And store_sales joins with store on ss_store_sk equals s_store_sk

  Scenario: Handle data quality and null values
    Given customer address country may contain null values
    When customer_country is null
    Then map to "UNKNOWN"
    
    And when store_sk is null for catalog_sales or web_sales
    Then keep as null to indicate non-store channels
    
    And exclude records where date is null
    And exclude records where customer is null
