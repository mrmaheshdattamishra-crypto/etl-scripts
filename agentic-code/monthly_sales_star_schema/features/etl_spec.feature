Feature: monthly_sales_star_schema

  Background: Source and Target Schema
    Given source table "store_sales" with columns:
      | column_name           | datatype    |
      | ss_sold_date_sk       | NUMBER      |
      | ss_item_sk            | NUMBER      |
      | ss_store_sk           | NUMBER      |
      | ss_quantity           | NUMBER      |
      | ss_sales_price        | NUMBER      |
      | ss_ext_sales_price    | NUMBER      |
      | ss_net_paid           | NUMBER      |
      | ss_net_profit         | NUMBER      |

    And source table "item" with columns:
      | column_name     | datatype |
      | i_item_sk       | NUMBER   |
      | i_item_id       | STRING   |
      | i_item_desc     | STRING   |
      | i_brand         | STRING   |
      | i_class         | STRING   |
      | i_category      | STRING   |
      | i_product_name  | STRING   |

    And source table "store" with columns:
      | column_name       | datatype |
      | s_store_sk        | NUMBER   |
      | s_store_id        | STRING   |
      | s_store_name      | STRING   |
      | s_city            | STRING   |
      | s_state           | STRING   |
      | s_market_desc     | STRING   |
      | s_division_name   | STRING   |

    And source table "date_dim" with columns:
      | column_name | datatype |
      | d_date_sk   | NUMBER   |
      | d_date      | DATE     |
      | d_year      | NUMBER   |
      | d_moy       | NUMBER   |
      | d_qoy       | NUMBER   |

    And target fact table "fact_monthly_sales" with columns:
      | column_name         | datatype |
      | date_key            | NUMBER   |
      | item_key            | NUMBER   |
      | store_key           | NUMBER   |
      | year                | NUMBER   |
      | month               | NUMBER   |
      | total_sales_amount  | NUMBER   |
      | total_quantity      | NUMBER   |
      | total_net_paid      | NUMBER   |
      | total_net_profit    | NUMBER   |

    And target dimension table "dim_item" with columns:
      | column_name  | datatype |
      | item_key     | NUMBER   |
      | item_id      | STRING   |
      | item_desc    | STRING   |
      | brand        | STRING   |
      | class        | STRING   |
      | category     | STRING   |
      | product_name | STRING   |

    And target dimension table "dim_store" with columns:
      | column_name   | datatype |
      | store_key     | NUMBER   |
      | store_id      | STRING   |
      | store_name    | STRING   |
      | city          | STRING   |
      | state         | STRING   |
      | market_desc   | STRING   |
      | division_name | STRING   |

    And target dimension table "dim_date" with columns:
      | column_name | datatype |
      | date_key    | NUMBER   |
      | date        | DATE     |
      | year        | NUMBER   |
      | month       | NUMBER   |
      | quarter     | NUMBER   |

  Scenario: Load dimension tables
    When loading "dim_item"
    Then select all columns from item table
    And map i_item_sk to item_key
    And map i_item_id to item_id
    And map i_item_desc to item_desc
    And map i_brand to brand
    And map i_class to class
    And map i_category to category
    And map i_product_name to product_name

    When loading "dim_store"
    Then select all columns from store table
    And map s_store_sk to store_key
    And map s_store_id to store_id
    And map s_store_name to store_name
    And map s_city to city
    And map s_state to state
    And map s_market_desc to market_desc
    And map s_division_name to division_name

    When loading "dim_date"
    Then select all columns from date_dim table
    And map d_date_sk to date_key
    And map d_date to date
    And map d_year to year
    And map d_moy to month
    And map d_qoy to quarter

  Scenario: Load fact table for monthly sales aggregation
    When loading "fact_monthly_sales"
    Then join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
    And join with item on ss_item_sk equals i_item_sk
    And join with store on ss_store_sk equals s_store_sk
    And group by d_date_sk, i_item_sk, s_store_sk, d_year, d_moy
    And map d_date_sk to date_key
    And map i_item_sk to item_key
    And map s_store_sk to store_key
    And map d_year to year
    And map d_moy to month
    And sum ss_ext_sales_price to total_sales_amount
    And sum ss_quantity to total_quantity
    And sum ss_net_paid to total_net_paid
    And sum ss_net_profit to total_net_profit