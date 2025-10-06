# Referenced similar ticket: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Leveraging existing star schema pattern for sales analysis

Feature: monthly_sales_star_schema
  As a business analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background: Source and Target Schema Definition
    Given source table "store_sales" with schema:
      | column_name           | datatype      |
      | ss_sold_date_sk       | NUMBER        |
      | ss_item_sk            | NUMBER        |
      | ss_store_sk           | NUMBER        |
      | ss_quantity           | NUMBER        |
      | ss_sales_price        | NUMBER        |
      | ss_ext_sales_price    | NUMBER        |
      | ss_net_paid           | NUMBER        |
      | ss_net_profit         | NUMBER        |

    And source table "date_dim" with schema:
      | column_name    | datatype |
      | d_date_sk      | NUMBER   |
      | d_date         | DATE     |
      | d_year         | NUMBER   |
      | d_moy          | NUMBER   |
      | d_month_seq    | NUMBER   |

    And source table "item" with schema:
      | column_name    | datatype |
      | i_item_sk      | NUMBER   |
      | i_item_id      | STRING   |
      | i_product_name | STRING   |
      | i_brand        | STRING   |
      | i_category     | STRING   |
      | i_class        | STRING   |

    And source table "store" with schema:
      | column_name      | datatype |
      | s_store_sk       | NUMBER   |
      | s_store_id       | STRING   |
      | s_store_name     | STRING   |
      | s_city           | STRING   |
      | s_state          | STRING   |
      | s_division_name  | STRING   |

    And target table "fact_monthly_sales" with schema:
      | column_name          | datatype |
      | date_sk              | NUMBER   |
      | item_sk              | NUMBER   |
      | store_sk             | NUMBER   |
      | sales_month          | STRING   |
      | sales_year           | NUMBER   |
      | total_quantity       | NUMBER   |
      | total_sales_amount   | NUMBER   |
      | total_net_paid       | NUMBER   |
      | total_net_profit     | NUMBER   |
      | avg_sales_price      | NUMBER   |

    And target table "dim_date_monthly" with schema:
      | column_name    | datatype |
      | date_sk        | NUMBER   |
      | sales_month    | STRING   |
      | sales_year     | NUMBER   |
      | month_number   | NUMBER   |
      | year_month     | STRING   |

    And target table "dim_item" with schema:
      | column_name    | datatype |
      | item_sk        | NUMBER   |
      | item_id        | STRING   |
      | product_name   | STRING   |
      | brand          | STRING   |
      | category       | STRING   |
      | class          | STRING   |

    And target table "dim_store" with schema:
      | column_name     | datatype |
      | store_sk        | NUMBER   |
      | store_id        | STRING   |
      | store_name      | STRING   |
      | city            | STRING   |
      | state           | STRING   |
      | division_name   | STRING   |

  Scenario: Build monthly sales fact table
    Given I need to join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    When I aggregate sales data by month, item, and store
    Then create fact_monthly_sales by grouping store_sales by ss_item_sk, ss_store_sk, d_year, and d_moy
    And calculate total_quantity as sum of ss_quantity
    And calculate total_sales_amount as sum of ss_ext_sales_price
    And calculate total_net_paid as sum of ss_net_paid
    And calculate total_net_profit as sum of ss_net_profit
    And calculate avg_sales_price as average of ss_sales_price
    And set sales_month as concatenation of d_year and d_moy with format YYYY-MM
    And set sales_year as d_year
    And set date_sk as d_date_sk for first day of each month
    And set item_sk as ss_item_sk
    And set store_sk as ss_store_sk

  Scenario: Build date dimension for monthly analysis
    Given I need date dimension aggregated by month
    When I process date_dim table
    Then create dim_date_monthly by selecting distinct combinations of d_year and d_moy
    And set date_sk as minimum d_date_sk for each month
    And set sales_month as concatenation of d_year and d_moy with format YYYY-MM
    And set sales_year as d_year
    And set month_number as d_moy
    And set year_month as concatenation of d_year and d_moy with dash separator

  Scenario: Build item dimension
    Given I need item attributes for analysis
    When I process item table
    Then create dim_item by copying all records from item table
    And map i_item_sk to item_sk
    And map i_item_id to item_id
    And map i_product_name to product_name
    And map i_brand to brand
    And map i_category to category
    And map i_class to class

  Scenario: Build store dimension
    Given I need store attributes for analysis
    When I process store table
    Then create dim_store by copying all records from store table
    And map s_store_sk to store_sk
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And map s_division_name to division_name

  Scenario: Establish star schema relationships
    Given the dimensional model is built
    Then fact_monthly_sales joins to dim_date_monthly on date_sk
    And fact_monthly_sales joins to dim_item on item_sk
    And fact_monthly_sales joins to dim_store on store_sk