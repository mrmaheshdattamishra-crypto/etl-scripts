# Referencing similar ticket SCRUM-9 which addressed the same monthly sales KPI requirements
# Leveraging existing star schema pattern for sales analysis

Feature: monthly_sales_star_schema
  As a data analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background: Source Tables Schema
    Given the following source tables exist:
      | table_name  | columns                                                           |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER, ss_net_profit:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_division_name:STRING, s_company_name:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER, d_quarter_name:STRING |

  Background: Target Schema
    Given the following target tables will be created:
      | table_name           | columns                                                        |
      | fact_monthly_sales   | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, sales_amount:NUMBER, quantity_sold:NUMBER, net_profit:NUMBER, transaction_count:NUMBER |
      | dim_item            | item_key:NUMBER, item_id:STRING, item_name:STRING, brand:STRING, category:STRING, class:STRING |
      | dim_store           | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, division:STRING, company:STRING |
      | dim_date            | date_key:NUMBER, date_value:DATE, year:NUMBER, month:NUMBER, month_name:STRING, quarter:STRING |

  Scenario: Create Item Dimension
    Given I need to populate the dim_item table
    When I extract data from the item table
    Then I should map the following fields:
      | source_field    | target_field |
      | i_item_sk       | item_key     |
      | i_item_id       | item_id      |
      | i_product_name  | item_name    |
      | i_brand         | brand        |
      | i_category      | category     |
      | i_class         | class        |

  Scenario: Create Store Dimension
    Given I need to populate the dim_store table
    When I extract data from the store table
    Then I should map the following fields:
      | source_field     | target_field |
      | s_store_sk       | store_key    |
      | s_store_id       | store_id     |
      | s_store_name     | store_name   |
      | s_city           | city         |
      | s_state          | state        |
      | s_division_name  | division     |
      | s_company_name   | company      |

  Scenario: Create Date Dimension
    Given I need to populate the dim_date table
    When I extract data from the date_dim table
    Then I should map the following fields:
      | source_field    | target_field  |
      | d_date_sk       | date_key      |
      | d_date          | date_value    |
      | d_year          | year          |
      | d_moy           | month         |
      | d_quarter_name  | quarter       |
    And I should derive month_name from the month number

  Scenario: Create Monthly Sales Fact Table
    Given I need to populate the fact_monthly_sales table
    When I join the following tables:
      | table       | join_condition                    |
      | store_sales | Primary fact table                |
      | date_dim    | ss_sold_date_sk = d_date_sk      |
      | item        | ss_item_sk = i_item_sk           |
      | store       | ss_store_sk = s_store_sk         |
    Then I should aggregate sales data by month, item, and store
    And I should map the following fields:
      | source_field      | target_field      | transformation                           |
      | d_date_sk         | date_key          | Use date dimension key                   |
      | i_item_sk         | item_key          | Use item dimension key                   |
      | s_store_sk        | store_key         | Use store dimension key                  |
      | ss_ext_sales_price| sales_amount      | Sum of extended sales price by month     |
      | ss_quantity       | quantity_sold     | Sum of quantity sold by month            |
      | ss_net_profit     | net_profit        | Sum of net profit by month               |
      | COUNT(*)          | transaction_count | Count of transactions by month           |

  Scenario: Enable Monthly Sales by Item Analysis
    Given the monthly sales star schema is populated
    When analysts query sales by item and month
    Then they should be able to aggregate sales_amount and quantity_sold from fact_monthly_sales
    And join with dim_item to get item details
    And join with dim_date to get month information

  Scenario: Enable Monthly Sales by Store Analysis
    Given the monthly sales star schema is populated
    When analysts query sales by store and month
    Then they should be able to aggregate sales_amount and quantity_sold from fact_monthly_sales
    And join with dim_store to get store details
    And join with dim_date to get month information