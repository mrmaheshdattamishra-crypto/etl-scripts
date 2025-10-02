# Referenced similar ticket: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Building upon established star schema pattern for sales analysis

Feature: monthly_sales_star_schema
  
  Scenario: Build star schema for monthly sales analysis
    
    Given source table "store_sales" with schema:
      | Column Name           | Data Type      |
      | ss_sold_date_sk       | NUMBER         |
      | ss_item_sk            | NUMBER         |
      | ss_store_sk           | NUMBER         |
      | ss_quantity           | NUMBER         |
      | ss_sales_price        | NUMBER         |
      | ss_ext_sales_price    | NUMBER         |
      | ss_net_paid           | NUMBER         |
      | ss_net_profit         | NUMBER         |
    
    And source table "date_dim" with schema:
      | Column Name    | Data Type |
      | d_date_sk      | NUMBER    |
      | d_date         | DATE      |
      | d_year         | NUMBER    |
      | d_moy          | NUMBER    |
      | d_month_seq    | NUMBER    |
    
    And source table "item" with schema:
      | Column Name      | Data Type |
      | i_item_sk        | NUMBER    |
      | i_item_id        | STRING    |
      | i_product_name   | STRING    |
      | i_brand          | STRING    |
      | i_category       | STRING    |
      | i_class          | STRING    |
    
    And source table "store" with schema:
      | Column Name      | Data Type |
      | s_store_sk       | NUMBER    |
      | s_store_id       | STRING    |
      | s_store_name     | STRING    |
      | s_city           | STRING    |
      | s_state          | STRING    |
      | s_division_name  | STRING    |
    
    When I create target table "fact_monthly_sales" with schema:
      | Column Name           | Data Type |
      | date_key              | NUMBER    |
      | item_key              | NUMBER    |
      | store_key             | NUMBER    |
      | sales_year            | NUMBER    |
      | sales_month           | NUMBER    |
      | total_quantity        | NUMBER    |
      | total_sales_amount    | NUMBER    |
      | total_net_paid        | NUMBER    |
      | total_net_profit      | NUMBER    |
      | transaction_count     | NUMBER    |
    
    And I create target table "dim_date_monthly" with schema:
      | Column Name    | Data Type |
      | date_key       | NUMBER    |
      | year_month     | STRING    |
      | year           | NUMBER    |
      | month          | NUMBER    |
      | month_name     | STRING    |
      | quarter        | NUMBER    |
    
    And I create target table "dim_item" with schema:
      | Column Name      | Data Type |
      | item_key         | NUMBER    |
      | item_id          | STRING    |
      | product_name     | STRING    |
      | brand            | STRING    |
      | category         | STRING    |
      | class            | STRING    |
    
    And I create target table "dim_store" with schema:
      | Column Name      | Data Type |
      | store_key        | NUMBER    |
      | store_id         | STRING    |
      | store_name       | STRING    |
      | city             | STRING    |
      | state            | STRING    |
      | division_name    | STRING    |
    
    Then I join tables using the following relationships:
      | Source Table | Target Table | Join Condition                    |
      | store_sales  | date_dim     | ss_sold_date_sk = d_date_sk      |
      | store_sales  | item         | ss_item_sk = i_item_sk           |
      | store_sales  | store        | ss_store_sk = s_store_sk         |
    
    And I apply the following mapping logic:
      | Target Table           | Target Column         | Mapping Logic                                           |
      | fact_monthly_sales     | date_key              | Use d_date_sk from date dimension                      |
      | fact_monthly_sales     | item_key              | Use i_item_sk from item dimension                      |
      | fact_monthly_sales     | store_key             | Use s_store_sk from store dimension                    |
      | fact_monthly_sales     | sales_year            | Extract year from d_year                               |
      | fact_monthly_sales     | sales_month           | Extract month from d_moy                               |
      | fact_monthly_sales     | total_quantity        | Sum ss_quantity grouped by year month item and store   |
      | fact_monthly_sales     | total_sales_amount    | Sum ss_ext_sales_price grouped by year month item and store |
      | fact_monthly_sales     | total_net_paid        | Sum ss_net_paid grouped by year month item and store   |
      | fact_monthly_sales     | total_net_profit      | Sum ss_net_profit grouped by year month item and store |
      | fact_monthly_sales     | transaction_count     | Count distinct ss_ticket_number grouped by year month item and store |
      | dim_date_monthly       | date_key              | Use d_date_sk as primary key                           |
      | dim_date_monthly       | year_month            | Concatenate d_year and d_moy with hyphen               |
      | dim_date_monthly       | year                  | Use d_year as is                                       |
      | dim_date_monthly       | month                 | Use d_moy as is                                        |
      | dim_date_monthly       | month_name            | Convert d_moy to month name using case statement       |
      | dim_date_monthly       | quarter               | Calculate quarter from d_moy using ceiling division by three |
      | dim_item               | item_key              | Use i_item_sk as primary key                           |
      | dim_item               | item_id               | Use i_item_id as is                                    |
      | dim_item               | product_name          | Use i_product_name as is                               |
      | dim_item               | brand                 | Use i_brand as is                                      |
      | dim_item               | category              | Use i_category as is                                   |
      | dim_item               | class                 | Use i_class as is                                      |
      | dim_store              | store_key             | Use s_store_sk as primary key                          |
      | dim_store              | store_id              | Use s_store_id as is                                   |
      | dim_store              | store_name            | Use s_store_name as is                                 |
      | dim_store              | city                  | Use s_city as is                                       |
      | dim_store              | state                 | Use s_state as is                                      |
      | dim_store              | division_name         | Use s_division_name as is                              |