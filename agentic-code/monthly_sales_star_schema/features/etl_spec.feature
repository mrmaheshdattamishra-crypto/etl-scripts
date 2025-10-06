# Referenced similar ticket SCRUM-9 for monthly sales analysis patterns
# Data Product: monthly_sales_star_schema

Feature: Monthly Sales Star Schema
  As a business analyst
  I want to analyze monthly sales data
  So that I can track sales performance by item and store on a monthly basis

  Background:
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER, ss_net_profit:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_country:STRING, s_market_desc:STRING, s_division_name:STRING, s_company_name:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER, d_quarter_name:STRING, d_day_name:STRING |

    And the following target tables will be created:
      | table_name           | columns |
      | fact_monthly_sales   | month_year_key:STRING, item_key:NUMBER, store_key:NUMBER, total_quantity:NUMBER, total_sales_amount:NUMBER, total_net_paid:NUMBER, total_net_profit:NUMBER, avg_sales_price:NUMBER, transaction_count:NUMBER |
      | dim_item            | item_key:NUMBER, item_id:STRING, item_name:STRING, item_description:STRING, brand:STRING, class:STRING, category:STRING |
      | dim_store           | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, country:STRING, market_description:STRING, division:STRING, company:STRING |
      | dim_month           | month_year_key:STRING, year:NUMBER, month:NUMBER, month_name:STRING, quarter:STRING, month_sequence:NUMBER |

  Scenario: Create dimension table for items
    Given I want to build dim_item table
    When I extract data from item table
    Then I should map the following fields:
      | source_field    | target_field        | transformation |
      | i_item_sk       | item_key           | direct mapping |
      | i_item_id       | item_id            | direct mapping |
      | i_product_name  | item_name          | direct mapping |
      | i_item_desc     | item_description   | direct mapping |
      | i_brand         | brand              | direct mapping |
      | i_class         | class              | direct mapping |
      | i_category      | category           | direct mapping |

  Scenario: Create dimension table for stores
    Given I want to build dim_store table
    When I extract data from store table
    Then I should map the following fields:
      | source_field     | target_field          | transformation |
      | s_store_sk       | store_key            | direct mapping |
      | s_store_id       | store_id             | direct mapping |
      | s_store_name     | store_name           | direct mapping |
      | s_city           | city                 | direct mapping |
      | s_state          | state                | direct mapping |
      | s_country        | country              | direct mapping |
      | s_market_desc    | market_description   | direct mapping |
      | s_division_name  | division             | direct mapping |
      | s_company_name   | company              | direct mapping |

  Scenario: Create dimension table for months
    Given I want to build dim_month table
    When I extract unique months from date_dim table
    Then I should map the following fields:
      | source_field     | target_field       | transformation |
      | d_year, d_moy    | month_year_key     | concatenate year and zero-padded month with hyphen |
      | d_year           | year               | direct mapping |
      | d_moy            | month              | direct mapping |
      | d_moy            | month_name         | convert month number to month name |
      | d_quarter_name   | quarter            | direct mapping |
      | d_month_seq      | month_sequence     | direct mapping |

  Scenario: Create fact table for monthly sales
    Given I want to build fact_monthly_sales table
    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I group by year, month, item_sk, and store_sk
    Then I should map the following fields:
      | source_field           | target_field         | transformation |
      | d_year, d_moy          | month_year_key       | concatenate year and zero-padded month with hyphen |
      | ss_item_sk             | item_key             | direct mapping |
      | ss_store_sk            | store_key            | direct mapping |
      | ss_quantity            | total_quantity       | sum all quantities for the month |
      | ss_ext_sales_price     | total_sales_amount   | sum all extended sales prices for the month |
      | ss_net_paid            | total_net_paid       | sum all net paid amounts for the month |
      | ss_net_profit          | total_net_profit     | sum all net profits for the month |
      | ss_sales_price         | avg_sales_price      | calculate average sales price for the month |
      | ss_ticket_number       | transaction_count    | count distinct transactions for the month |

  Scenario: Support KPI - Sales by item per month
    Given fact_monthly_sales table exists
    And dim_item table exists
    And dim_month table exists
    When I join fact_monthly_sales with dim_item on item_key
    And I join with dim_month on month_year_key
    Then I can analyze total_sales_amount by item_name and month_year_key

  Scenario: Support KPI - Sales by store per month
    Given fact_monthly_sales table exists
    And dim_store table exists
    And dim_month table exists
    When I join fact_monthly_sales with dim_store on store_key
    And I join with dim_month on month_year_key
    Then I can analyze total_sales_amount by store_name and month_year_key