# Referenced similar ticket: SCRUM-9 - CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT (similarity: 0.66)
# Leveraging existing star schema pattern for monthly sales KPI analysis

Feature: monthly_sales_star_schema
  As a data analyst
  I want to analyze monthly sales performance
  So that I can track sales by item and store on a monthly basis

  Background:
    Given the following source table schemas:
      | table_name  | column_name        | datatype    |
      | store_sales | ss_sold_date_sk    | NUMBER      |
      | store_sales | ss_item_sk         | NUMBER      |
      | store_sales | ss_store_sk        | NUMBER      |
      | store_sales | ss_quantity        | NUMBER      |
      | store_sales | ss_sales_price     | NUMBER      |
      | store_sales | ss_ext_sales_price | NUMBER      |
      | store_sales | ss_net_paid        | NUMBER      |
      | item        | i_item_sk          | NUMBER      |
      | item        | i_item_id          | STRING      |
      | item        | i_item_desc        | STRING      |
      | item        | i_brand            | STRING      |
      | item        | i_category         | STRING      |
      | item        | i_product_name     | STRING      |
      | store       | s_store_sk         | NUMBER      |
      | store       | s_store_id         | STRING      |
      | store       | s_store_name       | STRING      |
      | store       | s_city             | STRING      |
      | store       | s_state            | STRING      |
      | store       | s_country          | STRING      |
      | date_dim    | d_date_sk          | NUMBER      |
      | date_dim    | d_date             | DATE        |
      | date_dim    | d_year             | NUMBER      |
      | date_dim    | d_moy              | NUMBER      |
      | date_dim    | d_month_seq        | NUMBER      |

    And the target star schema includes:
      | table_name           | column_name           | datatype |
      | fact_monthly_sales   | date_key              | NUMBER   |
      | fact_monthly_sales   | item_key              | NUMBER   |
      | fact_monthly_sales   | store_key             | NUMBER   |
      | fact_monthly_sales   | sales_year            | NUMBER   |
      | fact_monthly_sales   | sales_month           | NUMBER   |
      | fact_monthly_sales   | month_seq             | NUMBER   |
      | fact_monthly_sales   | total_quantity        | NUMBER   |
      | fact_monthly_sales   | total_sales_amount    | NUMBER   |
      | fact_monthly_sales   | total_net_paid        | NUMBER   |
      | fact_monthly_sales   | transaction_count     | NUMBER   |
      | dim_item             | item_key              | NUMBER   |
      | dim_item             | item_id               | STRING   |
      | dim_item             | item_description      | STRING   |
      | dim_item             | brand_name            | STRING   |
      | dim_item             | category_name         | STRING   |
      | dim_item             | product_name          | STRING   |
      | dim_store            | store_key             | NUMBER   |
      | dim_store            | store_id              | STRING   |
      | dim_store            | store_name            | STRING   |
      | dim_store            | store_city            | STRING   |
      | dim_store            | store_state           | STRING   |
      | dim_store            | store_country         | STRING   |
      | dim_date_monthly     | date_key              | NUMBER   |
      | dim_date_monthly     | calendar_date         | DATE     |
      | dim_date_monthly     | year_number           | NUMBER   |
      | dim_date_monthly     | month_number          | NUMBER   |
      | dim_date_monthly     | month_sequence        | NUMBER   |
      | dim_date_monthly     | year_month_label      | STRING   |

  Scenario: Create dimension table for items
    Given the source table item
    When I transform the data
    Then the target table dim_item should be populated where:
      | target_column     | source_mapping           |
      | item_key          | i_item_sk               |
      | item_id           | i_item_id               |
      | item_description  | i_item_desc             |
      | brand_name        | i_brand                 |
      | category_name     | i_category              |
      | product_name      | i_product_name          |

  Scenario: Create dimension table for stores
    Given the source table store
    When I transform the data
    Then the target table dim_store should be populated where:
      | target_column | source_mapping |
      | store_key     | s_store_sk     |
      | store_id      | s_store_id     |
      | store_name    | s_store_name   |
      | store_city    | s_city         |
      | store_state   | s_state        |
      | store_country | s_country      |

  Scenario: Create dimension table for monthly dates
    Given the source table date_dim
    When I transform the data
    Then the target table dim_date_monthly should be populated where:
      | target_column      | source_mapping                                    |
      | date_key           | d_date_sk                                        |
      | calendar_date      | d_date                                           |
      | year_number        | d_year                                           |
      | month_number       | d_moy                                            |
      | month_sequence     | d_month_seq                                      |
      | year_month_label   | concatenate d_year and d_moy with dash separator |

  Scenario: Create fact table for monthly sales aggregation
    Given the source tables store_sales, date_dim, item, and store are joined
    When I join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
    And I join store_sales to item on ss_item_sk equals i_item_sk
    And I join store_sales to store on ss_store_sk equals s_store_sk
    And I group by d_date_sk, d_year, d_moy, d_month_seq, i_item_sk, s_store_sk
    Then the target table fact_monthly_sales should be populated where:
      | target_column        | source_mapping                              |
      | date_key             | d_date_sk                                  |
      | item_key             | i_item_sk                                  |
      | store_key            | s_store_sk                                 |
      | sales_year           | d_year                                     |
      | sales_month          | d_moy                                      |
      | month_seq            | d_month_seq                                |
      | total_quantity       | sum of ss_quantity                         |
      | total_sales_amount   | sum of ss_ext_sales_price                  |
      | total_net_paid       | sum of ss_net_paid                         |
      | transaction_count    | count of distinct ss_ticket_number         |