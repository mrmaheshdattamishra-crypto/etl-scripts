# Referenced similar ticket SCRUM-9 with identical requirements for monthly sales star schema
# Leveraging proven pattern for sales by item/month and sales by store/month KPIs

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data through a star schema
  So that I can track sales performance by item and store on a monthly basis

  Background: Source and target schema definitions
    Given the source tables with schema:
      | table_name  | column_name         | data_type    |
      | store_sales | ss_sold_date_sk     | NUMBER       |
      | store_sales | ss_item_sk          | NUMBER       |
      | store_sales | ss_store_sk         | NUMBER       |
      | store_sales | ss_quantity         | NUMBER       |
      | store_sales | ss_sales_price      | NUMBER       |
      | store_sales | ss_ext_sales_price  | NUMBER       |
      | store_sales | ss_net_paid         | NUMBER       |
      | item        | i_item_sk           | NUMBER       |
      | item        | i_item_id           | STRING       |
      | item        | i_item_desc         | STRING       |
      | item        | i_brand             | STRING       |
      | item        | i_category          | STRING       |
      | item        | i_product_name      | STRING       |
      | store       | s_store_sk          | NUMBER       |
      | store       | s_store_id          | STRING       |
      | store       | s_store_name        | STRING       |
      | store       | s_city              | STRING       |
      | store       | s_state             | STRING       |
      | date_dim    | d_date_sk           | NUMBER       |
      | date_dim    | d_date              | DATE         |
      | date_dim    | d_year              | NUMBER       |
      | date_dim    | d_moy               | NUMBER       |
      | date_dim    | d_month_seq         | NUMBER       |
    
    And the target fact table with schema:
      | table_name           | column_name      | data_type |
      | fact_monthly_sales   | date_key         | NUMBER    |
      | fact_monthly_sales   | item_key         | NUMBER    |
      | fact_monthly_sales   | store_key        | NUMBER    |
      | fact_monthly_sales   | sales_year       | NUMBER    |
      | fact_monthly_sales   | sales_month      | NUMBER    |
      | fact_monthly_sales   | total_quantity   | NUMBER    |
      | fact_monthly_sales   | total_sales      | NUMBER    |
      | fact_monthly_sales   | total_net_paid   | NUMBER    |
      | fact_monthly_sales   | transaction_count| NUMBER    |
    
    And the target dimension tables with schema:
      | table_name     | column_name    | data_type |
      | dim_item       | item_key       | NUMBER    |
      | dim_item       | item_id        | STRING    |
      | dim_item       | item_name      | STRING    |
      | dim_item       | brand          | STRING    |
      | dim_item       | category       | STRING    |
      | dim_store      | store_key      | NUMBER    |
      | dim_store      | store_id       | STRING    |
      | dim_store      | store_name     | STRING    |
      | dim_store      | city           | STRING    |
      | dim_store      | state          | STRING    |
      | dim_date       | date_key       | NUMBER    |
      | dim_date       | full_date      | DATE      |
      | dim_date       | year           | NUMBER    |
      | dim_date       | month          | NUMBER    |
      | dim_date       | month_name     | STRING    |

  Scenario: Create item dimension table
    Given source table item
    When transforming data for dim_item
    Then map item_key from i_item_sk
    And map item_id from i_item_id
    And map item_name from i_product_name
    And map brand from i_brand
    And map category from i_category
    And filter out records where i_item_sk is null

  Scenario: Create store dimension table
    Given source table store
    When transforming data for dim_store
    Then map store_key from s_store_sk
    And map store_id from s_store_id
    And map store_name from s_store_name
    And map city from s_city
    And map state from s_state
    And filter out records where s_store_sk is null

  Scenario: Create date dimension table
    Given source table date_dim
    When transforming data for dim_date
    Then map date_key from d_date_sk
    And map full_date from d_date
    And map year from d_year
    And map month from d_moy
    And map month_name by converting d_moy to month name using case statement
    And filter out records where d_date_sk is null

  Scenario: Create monthly sales fact table
    Given source tables store_sales, date_dim, item, and store
    When joining tables for fact_monthly_sales
    Then join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And join store_sales to item on ss_item_sk equals i_item_sk
    And join store_sales to store on ss_store_sk equals s_store_sk
    And group by d_date_sk, i_item_sk, s_store_sk, d_year, and d_moy
    And map date_key from d_date_sk
    And map item_key from i_item_sk
    And map store_key from s_store_sk
    And map sales_year from d_year
    And map sales_month from d_moy
    And map total_quantity by summing ss_quantity
    And map total_sales by summing ss_ext_sales_price
    And map total_net_paid by summing ss_net_paid
    And map transaction_count by counting distinct ss_ticket_number
    And filter out records where any key field is null