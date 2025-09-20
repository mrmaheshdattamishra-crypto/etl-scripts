Feature: Sales Analytics Data Product

  Scenario: sales_analytics data product specification
    Given I have the following source tables with their schemas:
      | table_name        | columns                                                                                                                                           |
      | store_sales       | ss_sold_date_sk:NUMBER, ss_sold_time_sk:NUMBER, ss_item_sk:NUMBER, ss_customer_sk:NUMBER, ss_store_sk:NUMBER, ss_quantity:NUMBER, ss_sales_price:NUMBER, ss_ext_sales_price:NUMBER, ss_net_profit:NUMBER |
      | date_dim          | d_date_sk:NUMBER, d_date_id:STRING, d_date:DATE, d_year:NUMBER, d_month_seq:NUMBER, d_quarter_seq:NUMBER, d_day_name:STRING, d_quarter_name:STRING |
      | store             | s_store_sk:NUMBER, s_store_id:STRING, s_store_name:STRING, s_city:STRING, s_state:STRING, s_country:STRING, s_market_desc:STRING           |
      | customer_address  | ca_address_sk:NUMBER, ca_address_id:STRING, ca_city:STRING, ca_county:STRING, ca_state:STRING, ca_zip:STRING, ca_country:STRING            |

    And I want to create the following target table schema:
      | table_name        | columns                                                                                                                                           |
      | sales_summary     | sale_date:DATE, store_name:STRING, store_city:STRING, store_state:STRING, total_sales:NUMBER, total_quantity:NUMBER, total_profit:NUMBER, transaction_count:NUMBER |

    When I define the join relationships:
      | source_table  | target_table      | join_condition                        |
      | store_sales   | date_dim          | ss_sold_date_sk = d_date_sk          |
      | store_sales   | store             | ss_store_sk = s_store_sk             |
      | store_sales   | customer_address  | ss_addr_sk = ca_address_sk           |

    Then I apply the following mapping logic:
      | target_column     | source_mapping                                      |
      | sale_date         | extract date from date dimension table             |
      | store_name        | get store name from store table                     |
      | store_city        | get store city from store table                     |
      | store_state       | get store state from store table                    |
      | total_sales       | sum all extended sales prices for each date and store |
      | total_quantity    | sum all quantities sold for each date and store    |
      | total_profit      | sum all net profit for each date and store         |
      | transaction_count | count distinct ticket numbers for each date and store |

    And I group the results by:
      | grouping_columns |
      | sale_date        |
      | store_name       |
      | store_city       |
      | store_state      |

    And I filter the data where:
      | filter_conditions                    |
      | sale date is not null                |
      | store key is not null                |
      | extended sales price is greater than zero |