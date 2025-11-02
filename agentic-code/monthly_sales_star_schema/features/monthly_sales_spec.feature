# Referenced similar ticket: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Leveraging existing star schema pattern for monthly sales analysis

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales by item and store
  So that I can track sales performance across different dimensions

  Background:
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER, ss_net_profit:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_division_name:STRING, s_company_name:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER, d_quarter_name:STRING |

    And the following target tables will be created:
      | table_name           | columns |
      | fact_monthly_sales   | date_key:NUMBER, item_key:NUMBER, store_key:NUMBER, sales_month:STRING, total_quantity:NUMBER, total_sales_amount:NUMBER, total_net_paid:NUMBER, total_net_profit:NUMBER, average_sales_price:NUMBER |
      | dim_item_monthly     | item_key:NUMBER, item_id:STRING, item_description:STRING, brand_name:STRING, category_name:STRING, product_name:STRING |
      | dim_store_monthly    | store_key:NUMBER, store_id:STRING, store_name:STRING, city:STRING, state:STRING, division_name:STRING, company_name:STRING |
      | dim_date_monthly     | date_key:NUMBER, sales_month:STRING, year:NUMBER, month_number:NUMBER, quarter_name:STRING, month_sequence:NUMBER |

  Scenario: Create monthly sales fact table
    Given I need to aggregate sales data by month, item, and store
    When I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And I group by d_year, d_moy, ss_item_sk, and ss_store_sk
    Then I should create fact_monthly_sales with:
      | mapping |
      | date_key from d_date_sk |
      | item_key from ss_item_sk |
      | store_key from ss_store_sk |
      | sales_month concatenated from d_year and d_moy as 'YYYY-MM' format |
      | total_quantity as sum of ss_quantity |
      | total_sales_amount as sum of ss_ext_sales_price |
      | total_net_paid as sum of ss_net_paid |
      | total_net_profit as sum of ss_net_profit |
      | average_sales_price as average of ss_sales_price |

  Scenario: Create item dimension for monthly analysis
    Given I need item details for monthly sales analysis
    When I select distinct items from item table
    Then I should create dim_item_monthly with:
      | mapping |
      | item_key from i_item_sk |
      | item_id from i_item_id |
      | item_description from i_item_desc |
      | brand_name from i_brand |
      | category_name from i_category |
      | product_name from i_product_name |

  Scenario: Create store dimension for monthly analysis
    Given I need store details for monthly sales analysis
    When I select distinct stores from store table
    Then I should create dim_store_monthly with:
      | mapping |
      | store_key from s_store_sk |
      | store_id from s_store_id |
      | store_name from s_store_name |
      | city from s_city |
      | state from s_state |
      | division_name from s_division_name |
      | company_name from s_company_name |

  Scenario: Create date dimension for monthly analysis
    Given I need date details aggregated by month
    When I select distinct year-month combinations from date_dim
    And I group by d_year and d_moy
    Then I should create dim_date_monthly with:
      | mapping |
      | date_key from minimum d_date_sk for each month |
      | sales_month concatenated from d_year and d_moy as 'YYYY-MM' format |
      | year from d_year |
      | month_number from d_moy |
      | quarter_name from d_quarter_name |
      | month_sequence from d_month_seq |

  Scenario: Enable sales by item per month KPI
    Given the star schema is created
    When I query fact_monthly_sales joined with dim_item_monthly and dim_date_monthly
    Then I can analyze total sales amount, quantity, and profit by item for each month
    And I can filter by brand, category, or specific items
    And I can trend analysis across multiple months for each item

  Scenario: Enable sales by store per month KPI
    Given the star schema is created
    When I query fact_monthly_sales joined with dim_store_monthly and dim_date_monthly
    Then I can analyze total sales amount, quantity, and profit by store for each month
    And I can filter by city, state, division, or company
    And I can compare store performance across different months