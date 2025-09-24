Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales data
  So that I can track sales performance by item and store

  Background:
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_company_name:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER |
    
    And the following target tables will be created:
      | table_name      | columns |
      | fact_sales      | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, sales_quantity:NUMBER, sales_amount:NUMBER, net_paid:NUMBER |
      | dim_item        | item_key:NUMBER, item_id:STRING, item_description:STRING, brand:STRING, item_class:STRING, category:STRING, product_name:STRING |
      | dim_store       | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, company_name:STRING |
      | dim_date        | date_key:NUMBER, date:DATE, year:NUMBER, month:NUMBER, month_sequence:NUMBER |

  Scenario: Extract and transform item dimension
    Given I have the item source table
    When I transform the data
    Then I create dim_item with item_key mapped from i_item_sk
    And item_id mapped from i_item_id
    And item_description mapped from i_item_desc
    And brand mapped from i_brand
    And item_class mapped from i_class
    And category mapped from i_category
    And product_name mapped from i_product_name

  Scenario: Extract and transform store dimension
    Given I have the store source table
    When I transform the data
    Then I create dim_store with store_key mapped from s_store_sk
    And store_id mapped from s_store_id
    And store_name mapped from s_store_name
    And city mapped from s_city
    And state mapped from s_state
    And company_name mapped from s_company_name

  Scenario: Extract and transform date dimension
    Given I have the date_dim source table
    When I transform the data
    Then I create dim_date with date_key mapped from d_date_sk
    And date mapped from d_date
    And year mapped from d_year
    And month mapped from d_moy
    And month_sequence mapped from d_month_seq

  Scenario: Extract and transform sales fact table
    Given I have the store_sales source table
    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I join with item on ss_item_sk equals i_item_sk
    And I join with store on ss_store_sk equals s_store_sk
    Then I create fact_sales with date_key mapped from ss_sold_date_sk
    And item_key mapped from ss_item_sk
    And store_key mapped from ss_store_sk
    And sales_quantity mapped from ss_quantity
    And sales_amount mapped from ss_ext_sales_price
    And net_paid mapped from ss_net_paid

  Scenario: Create sales by item and month view
    Given I have fact_sales joined with dim_item and dim_date
    When I group by item_key and month and year
    Then I calculate total sales_amount as monthly_item_sales
    And total sales_quantity as monthly_item_quantity
    And create sales_by_item_month table

  Scenario: Create sales by store and month view
    Given I have fact_sales joined with dim_store and dim_date
    When I group by store_key and month and year
    Then I calculate total sales_amount as monthly_store_sales
    And total sales_quantity as monthly_store_quantity
    And create sales_by_store_month table