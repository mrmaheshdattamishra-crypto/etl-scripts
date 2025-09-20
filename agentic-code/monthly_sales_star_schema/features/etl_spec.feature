Feature: monthly_sales_star_schema
  
  Scenario: Build star schema for monthly sales analysis
    
    Given source table "store_sales" with schema:
      | column_name | data_type |
      | ss_sold_date_sk | NUMBER |
      | ss_item_sk | NUMBER |
      | ss_store_sk | NUMBER |
      | ss_quantity | NUMBER |
      | ss_sales_price | NUMBER |
      | ss_ext_sales_price | NUMBER |
      | ss_net_paid | NUMBER |
      | ss_net_profit | NUMBER |
    
    And source table "item" with schema:
      | column_name | data_type |
      | i_item_sk | NUMBER |
      | i_item_id | STRING |
      | i_item_desc | STRING |
      | i_brand | STRING |
      | i_class | STRING |
      | i_category | STRING |
      | i_product_name | STRING |
    
    And source table "store" with schema:
      | column_name | data_type |
      | s_store_sk | NUMBER |
      | s_store_id | STRING |
      | s_store_name | STRING |
      | s_city | STRING |
      | s_state | STRING |
      | s_division_name | STRING |
      | s_company_name | STRING |
    
    And source table "date_dim" with schema:
      | column_name | data_type |
      | d_date_sk | NUMBER |
      | d_date | DATE |
      | d_year | NUMBER |
      | d_moy | NUMBER |
      | d_month_seq | NUMBER |
      | d_quarter_name | STRING |
    
    When I join store_sales to item on ss_item_sk equals i_item_sk
    And I join store_sales to store on ss_store_sk equals s_store_sk  
    And I join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    
    Then I create target table "fact_monthly_sales" with schema:
      | column_name | data_type |
      | sales_month_key | STRING |
      | item_key | NUMBER |
      | store_key | NUMBER |
      | year | NUMBER |
      | month | NUMBER |
      | total_quantity | NUMBER |
      | total_sales_amount | NUMBER |
      | total_net_paid | NUMBER |
      | total_net_profit | NUMBER |
      | transaction_count | NUMBER |
    
    And I create target table "dim_item" with schema:
      | column_name | data_type |
      | item_key | NUMBER |
      | item_id | STRING |
      | item_description | STRING |
      | brand | STRING |
      | class | STRING |
      | category | STRING |
      | product_name | STRING |
    
    And I create target table "dim_store" with schema:
      | column_name | data_type |
      | store_key | NUMBER |
      | store_id | STRING |
      | store_name | STRING |
      | city | STRING |
      | state | STRING |
      | division_name | STRING |
      | company_name | STRING |
    
    And I create target table "dim_date_month" with schema:
      | column_name | data_type |
      | sales_month_key | STRING |
      | year | NUMBER |
      | month | NUMBER |
      | month_name | STRING |
      | quarter_name | STRING |
    
    And I map the following transformations:
      | target_table | target_column | mapping_logic |
      | fact_monthly_sales | sales_month_key | concatenate year and month with hyphen from date dimension |
      | fact_monthly_sales | item_key | item surrogate key from store sales |
      | fact_monthly_sales | store_key | store surrogate key from store sales |
      | fact_monthly_sales | year | year from date dimension |
      | fact_monthly_sales | month | month of year from date dimension |
      | fact_monthly_sales | total_quantity | sum of quantity by item and month |
      | fact_monthly_sales | total_sales_amount | sum of extended sales price by item and month |
      | fact_monthly_sales | total_net_paid | sum of net paid amount by item and month |
      | fact_monthly_sales | total_net_profit | sum of net profit by item and month |
      | fact_monthly_sales | transaction_count | count of distinct transactions by item and month |
      | dim_item | item_key | item surrogate key from item table |
      | dim_item | item_id | item identifier from item table |
      | dim_item | item_description | item description from item table |
      | dim_item | brand | brand name from item table |
      | dim_item | class | class name from item table |
      | dim_item | category | category name from item table |
      | dim_item | product_name | product name from item table |
      | dim_store | store_key | store surrogate key from store table |
      | dim_store | store_id | store identifier from store table |
      | dim_store | store_name | store name from store table |
      | dim_store | city | city from store table |
      | dim_store | state | state from store table |
      | dim_store | division_name | division name from store table |
      | dim_store | company_name | company name from store table |
      | dim_date_month | sales_month_key | concatenate year and month with hyphen |
      | dim_date_month | year | year from date dimension |
      | dim_date_month | month | month of year from date dimension |
      | dim_date_month | month_name | derive month name from month number |
      | dim_date_month | quarter_name | quarter name from date dimension |