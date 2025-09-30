# Referenced similar ticket: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Leveraging established patterns for star schema monthly sales analysis

Feature: monthly_sales_star_schema

Background:
  Given the following source tables exist:
    | table_name  | columns                                                           | datatypes                                    |
    | store_sales | ss_sold_date_sk, ss_item_sk, ss_store_sk, ss_ext_sales_price, ss_quantity, ss_net_profit | NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER |
    | date_dim    | d_date_sk, d_date, d_year, d_moy, d_month_seq                   | NUMBER, DATE, NUMBER, NUMBER, NUMBER         |
    | item        | i_item_sk, i_item_id, i_product_name, i_category, i_brand      | NUMBER, STRING, STRING, STRING, STRING       |
    | store       | s_store_sk, s_store_id, s_store_name, s_city, s_state          | NUMBER, STRING, STRING, STRING, STRING       |

  And the following target star schema exists:
    | table_name        | columns                                                         | datatypes                                    |
    | fact_monthly_sales| month_key, item_key, store_key, total_sales, total_quantity, total_profit, avg_sales_price | STRING, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER |
    | dim_month         | month_key, year, month, month_name, quarter                     | STRING, NUMBER, NUMBER, STRING, NUMBER       |
    | dim_item          | item_key, item_id, product_name, category, brand               | NUMBER, STRING, STRING, STRING, STRING       |
    | dim_store         | store_key, store_id, store_name, city, state                   | NUMBER, STRING, STRING, STRING, STRING       |

Scenario: Build monthly sales fact table
  When I aggregate store sales data by month, item, and store
  Then I create fact_monthly_sales with the following logic:
    | field              | mapping_logic                                                   |
    | month_key          | Concatenate year and month as YYYY-MM format from date dimension |
    | item_key           | Use item surrogate key from item dimension                      |
    | store_key          | Use store surrogate key from store dimension                    |
    | total_sales        | Sum of extended sales price for the month, item, and store combination |
    | total_quantity     | Sum of quantity sold for the month, item, and store combination |
    | total_profit       | Sum of net profit for the month, item, and store combination    |
    | avg_sales_price    | Average of extended sales price for the month, item, and store combination |

Scenario: Build month dimension
  When I extract unique month combinations from date dimension
  Then I create dim_month with the following logic:
    | field       | mapping_logic                                           |
    | month_key   | Concatenate year and month as YYYY-MM format           |
    | year        | Extract year from date dimension                        |
    | month       | Extract month number from date dimension                |
    | month_name  | Convert month number to month name                      |
    | quarter     | Calculate quarter from month number                     |

Scenario: Build item dimension
  When I select item attributes from item table
  Then I create dim_item with the following logic:
    | field        | mapping_logic                              |
    | item_key     | Use item surrogate key                     |
    | item_id      | Use item business key                      |
    | product_name | Use product name from item table           |
    | category     | Use category from item table               |
    | brand        | Use brand from item table                  |

Scenario: Build store dimension
  When I select store attributes from store table
  Then I create dim_store with the following logic:
    | field      | mapping_logic                            |
    | store_key  | Use store surrogate key                  |
    | store_id   | Use store business key                   |
    | store_name | Use store name from store table          |
    | city       | Use city from store table                |
    | state      | Use state from store table               |

Scenario: Join relationships for fact table creation
  When I join the source tables for fact table
  Then I use the following join logic:
    | join_type | left_table   | right_table | join_condition                    |
    | INNER     | store_sales  | date_dim    | ss_sold_date_sk = d_date_sk      |
    | INNER     | store_sales  | item        | ss_item_sk = i_item_sk           |
    | INNER     | store_sales  | store       | ss_store_sk = s_store_sk         |