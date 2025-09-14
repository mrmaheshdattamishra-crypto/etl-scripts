Feature: monthly_sales_star_schema
  Data product for analyzing monthly sales with KPIs for sales by item/month and sales by store/month

  Background: Source Schema Definition
    Given the following source tables exist:
      | table_name  | columns |
      | store_sales | ss_sold_date_sk:NUMBER, ss_item_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_paid:NUMBER, ss_net_profit:NUMBER |
      | item        | i_item_sk:NUMBER, i_item_id:STRING, i_item_desc:STRING, i_brand:STRING, i_class:STRING, i_category:STRING, i_product_name:STRING |
      | store       | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_country:STRING |
      | date_dim    | d_date_sk:NUMBER, d_date:DATE, d_year:NUMBER, d_moy:NUMBER, d_month_seq:NUMBER, d_quarter_name:STRING |

  Scenario: Create fact table for monthly sales
    Given I need to create fact_monthly_sales table
    When I define the target schema as:
      | column_name           | datatype | description |
      | date_key             | NUMBER   | Foreign key to date dimension |
      | item_key             | NUMBER   | Foreign key to item dimension |
      | store_key            | NUMBER   | Foreign key to store dimension |
      | year_month           | STRING   | Year and month in YYYY-MM format |
      | total_quantity       | NUMBER   | Sum of quantities sold |
      | total_sales_amount   | NUMBER   | Sum of extended sales price |
      | total_net_paid       | NUMBER   | Sum of net paid amount |
      | total_net_profit     | NUMBER   | Sum of net profit |
    Then I will populate the fact table by:
      | mapping_logic |
      | Join store_sales with date_dim on ss_sold_date_sk equals d_date_sk |
      | Group by d_date_sk, ss_item_sk, ss_store_sk, and year-month combination |
      | Calculate year_month as concatenation of d_year and d_moy with hyphen separator |
      | Sum ss_quantity as total_quantity |
      | Sum ss_ext_sales_price as total_sales_amount |
      | Sum ss_net_paid as total_net_paid |
      | Sum ss_net_profit as total_net_profit |

  Scenario: Create item dimension table
    Given I need to create dim_item table
    When I define the target schema as:
      | column_name    | datatype | description |
      | item_key       | NUMBER   | Primary key from source |
      | item_id        | STRING   | Business key for item |
      | item_name      | STRING   | Product name |
      | item_description | STRING | Item description |
      | brand          | STRING   | Brand name |
      | category       | STRING   | Item category |
      | class          | STRING   | Item class |
    Then I will populate the dimension table by:
      | mapping_logic |
      | Select all records from item table |
      | Map i_item_sk to item_key |
      | Map i_item_id to item_id |
      | Map i_product_name to item_name |
      | Map i_item_desc to item_description |
      | Map i_brand to brand |
      | Map i_category to category |
      | Map i_class to class |

  Scenario: Create store dimension table
    Given I need to create dim_store table
    When I define the target schema as:
      | column_name   | datatype | description |
      | store_key     | NUMBER   | Primary key from source |
      | store_id      | STRING   | Business key for store |
      | store_name    | STRING   | Store name |
      | city          | STRING   | Store city |
      | state         | STRING   | Store state |
      | country       | STRING   | Store country |
    Then I will populate the dimension table by:
      | mapping_logic |
      | Select all records from store table |
      | Map s_store_sk to store_key |
      | Map s_store_id to store_id |
      | Map s_store_name to store_name |
      | Map s_city to city |
      | Map s_state to state |
      | Map s_country to country |

  Scenario: Create date dimension table
    Given I need to create dim_date table
    When I define the target schema as:
      | column_name     | datatype | description |
      | date_key        | NUMBER   | Primary key from source |
      | date_value      | DATE     | Actual date |
      | year            | NUMBER   | Year |
      | month           | NUMBER   | Month number |
      | year_month      | STRING   | Year and month in YYYY-MM format |
      | quarter_name    | STRING   | Quarter name |
    Then I will populate the dimension table by:
      | mapping_logic |
      | Select all records from date_dim table |
      | Map d_date_sk to date_key |
      | Map d_date to date_value |
      | Map d_year to year |
      | Map d_moy to month |
      | Create year_month by concatenating d_year and d_moy with hyphen separator |
      | Map d_quarter_name to quarter_name |

  Scenario: Define table relationships
    Given the star schema tables are created
    When I define the relationships:
      | fact_table        | dimension_table | join_condition |
      | fact_monthly_sales | dim_date       | fact_monthly_sales.date_key = dim_date.date_key |
      | fact_monthly_sales | dim_item       | fact_monthly_sales.item_key = dim_item.item_key |
      | fact_monthly_sales | dim_store      | fact_monthly_sales.store_key = dim_store.store_key |
    Then the star schema will support the following KPIs:
      | kpi_name           | calculation_logic |
      | Sales by item/month | Sum total_sales_amount from fact_monthly_sales grouped by item dimensions and year_month |
      | Sales by store/month | Sum total_sales_amount from fact_monthly_sales grouped by store dimensions and year_month |