Feature: sales_summary_by_channel

Background:
    Given source table "store_sales" with schema:
        | Column                | Type         |
        | ss_sold_date_sk       | NUMBER       |
        | ss_item_sk            | NUMBER       |
        | ss_customer_sk        | NUMBER       |
        | ss_store_sk           | NUMBER       |
        | ss_quantity           | NUMBER       |
        | ss_sales_price        | NUMBER       |
        | ss_ext_sales_price    | NUMBER       |
        | ss_net_paid           | NUMBER       |
        | ss_net_profit         | NUMBER       |

    And source table "web_sales" with schema:
        | Column                | Type         |
        | ws_sold_date_sk       | NUMBER       |
        | ws_item_sk            | NUMBER       |
        | ws_bill_customer_sk   | NUMBER       |
        | ws_web_site_sk        | NUMBER       |
        | ws_quantity           | NUMBER       |
        | ws_sales_price        | NUMBER       |
        | ws_ext_sales_price    | NUMBER       |
        | ws_net_paid           | NUMBER       |
        | ws_net_profit         | NUMBER       |

    And source table "catalog_sales" with schema:
        | Column                | Type         |
        | cs_sold_date_sk       | NUMBER       |
        | cs_item_sk            | NUMBER       |
        | cs_bill_customer_sk   | NUMBER       |
        | cs_call_center_sk     | NUMBER       |
        | cs_quantity           | NUMBER       |
        | cs_sales_price        | NUMBER       |
        | cs_ext_sales_price    | NUMBER       |
        | cs_net_paid           | NUMBER       |
        | cs_net_profit         | NUMBER       |

    And source table "date_dim" with schema:
        | Column                | Type         |
        | d_date_sk             | NUMBER       |
        | d_date                | DATE         |
        | d_year                | NUMBER       |
        | d_moy                 | NUMBER       |
        | d_qoy                 | NUMBER       |
        | d_day_name            | STRING       |
        | d_quarter_name        | STRING       |

    And source table "store" with schema:
        | Column                | Type         |
        | s_store_sk            | NUMBER       |
        | s_store_name          | STRING       |
        | s_state               | STRING       |
        | s_city                | STRING       |

    And source table "web_site" with schema:
        | Column                | Type         |
        | web_site_sk           | NUMBER       |
        | web_name              | STRING       |
        | web_state             | STRING       |
        | web_city              | STRING       |

    And source table "call_center" with schema:
        | Column                | Type         |
        | cc_call_center_sk     | NUMBER       |
        | cc_name               | STRING       |
        | cc_state              | STRING       |
        | cc_city               | STRING       |

    And target table "sales_summary_by_channel" with schema:
        | Column                | Type         |
        | sales_date            | DATE         |
        | year                  | NUMBER       |
        | month                 | NUMBER       |
        | quarter               | NUMBER       |
        | day_name              | STRING       |
        | channel               | STRING       |
        | channel_location      | STRING       |
        | channel_state         | STRING       |
        | total_quantity        | NUMBER       |
        | total_sales_amount    | NUMBER       |
        | total_net_paid        | NUMBER       |
        | total_net_profit      | NUMBER       |
        | transaction_count     | NUMBER       |
        | avg_sales_price       | NUMBER       |

Scenario: Transform store sales data
    When processing store sales transactions
    Then join store_sales with date_dim on sold date key equals date surrogate key
    And join store_sales with store on store surrogate key equals store surrogate key
    And map sales date to the actual date from date dimension
    And map year to the year from date dimension
    And map month to the month from date dimension
    And map quarter to the quarter from date dimension
    And map day name to the day name from date dimension
    And map channel to literal value store
    And map channel location to store name
    And map channel state to store state
    And aggregate total quantity by summing quantity
    And aggregate total sales amount by summing extended sales price
    And aggregate total net paid by summing net paid
    And aggregate total net profit by summing net profit
    And aggregate transaction count by counting distinct ticket numbers
    And calculate average sales price by dividing total sales amount by total quantity

Scenario: Transform web sales data
    When processing web sales transactions
    Then join web_sales with date_dim on sold date key equals date surrogate key
    And join web_sales with web_site on web site surrogate key equals web site surrogate key
    And map sales date to the actual date from date dimension
    And map year to the year from date dimension
    And map month to the month from date dimension
    And map quarter to the quarter from date dimension
    And map day name to the day name from date dimension
    And map channel to literal value web
    And map channel location to web site name
    And map channel state to web site state
    And aggregate total quantity by summing quantity
    And aggregate total sales amount by summing extended sales price
    And aggregate total net paid by summing net paid
    And aggregate total net profit by summing net profit
    And aggregate transaction count by counting distinct order numbers
    And calculate average sales price by dividing total sales amount by total quantity

Scenario: Transform catalog sales data
    When processing catalog sales transactions
    Then join catalog_sales with date_dim on sold date key equals date surrogate key
    And join catalog_sales with call_center on call center surrogate key equals call center surrogate key
    And map sales date to the actual date from date dimension
    And map year to the year from date dimension
    And map month to the month from date dimension
    And map quarter to the quarter from date dimension
    And map day name to the day name from date dimension
    And map channel to literal value catalog
    And map channel location to call center name
    And map channel state to call center state
    And aggregate total quantity by summing quantity
    And aggregate total sales amount by summing extended sales price
    And aggregate total net paid by summing net paid
    And aggregate total net profit by summing net profit
    And aggregate transaction count by counting distinct order numbers
    And calculate average sales price by dividing total sales amount by total quantity

Scenario: Combine all channel data
    When consolidating sales data across channels
    Then union all transformed store sales data
    And union all transformed web sales data
    And union all transformed catalog sales data
    And group by sales date year month quarter day name channel channel location and channel state
    And aggregate all metrics by summing quantities amounts paid profit and transaction counts
    And recalculate average sales price for combined data