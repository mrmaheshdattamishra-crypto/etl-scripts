Feature: customer_sales_analytics Test Plan

  Background: Test Data Setup
    Given synthetic data is generated for source tables:
      | table_name         | records |
      | customer           | 1000    |
      | web_sales          | 5000    |
      | date_dim           | 365     |
      | item               | 500     |
      | customer_address   | 800     |

    And customer table contains:
      | c_customer_sk | c_customer_id | c_current_addr_sk | c_first_name | c_last_name | c_preferred_cust_flag | c_birth_day | c_birth_month | c_birth_year | c_email_address          |
      | 1001          | CUST001       | 2001              | John         | Smith       | Y                     | 15          | 6             | 1980         | john.smith@email.com     |
      | 1002          | CUST002       | 2002              | Jane         | Doe         | N                     | 22          | 11            | 1985         | jane.doe@email.com       |
      | 1003          | CUST003       | 2003              | Bob          | Johnson     | Y                     | 8           | 3             | 1975         | bob.johnson@email.com    |

    And web_sales table contains:
      | ws_sold_date_sk | ws_item_sk | ws_bill_customer_sk | ws_order_number | ws_quantity | ws_ext_sales_price | ws_net_profit |
      | 3001            | 4001       | 1001                | 100001          | 2           | 150.00             | 30.00         |
      | 3002            | 4002       | 1001                | 100002          | 1           | 75.00              | 15.00         |
      | 3003            | 4001       | 1002                | 100003          | 3           | 225.00             | 45.00         |
      | 3004            | 4003       | 1003                | 100004          | 1           | 100.00             | 20.00         |

    And date_dim table contains:
      | d_date_sk | d_date     | d_year | d_moy | d_dom |
      | 3001      | 2023-01-15 | 2023   | 1     | 15    |
      | 3002      | 2023-02-20 | 2023   | 2     | 20    |
      | 3003      | 2023-03-10 | 2023   | 3     | 10    |
      | 3004      | 2023-04-05 | 2023   | 4     | 5     |

    And item table contains:
      | i_item_sk | i_item_id | i_category   | i_brand    |
      | 4001      | ITEM001   | Electronics  | BrandA     |
      | 4002      | ITEM002   | Clothing     | BrandB     |
      | 4003      | ITEM003   | Electronics  | BrandA     |

    And customer_address table contains:
      | ca_address_sk | ca_city    | ca_state | ca_country |
      | 2001          | New York   | NY       | USA        |
      | 2002          | Los Angeles| CA       | USA        |
      | 2003          | Chicago    | IL       | USA        |

  Scenario: Validate customer mapping and full name concatenation
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | customer_id | full_name   |
      | 1001         | CUST001     | John Smith  |
      | 1002         | CUST002     | Jane Doe    |
      | 1003         | CUST003     | Bob Johnson |

  Scenario: Validate birth date conversion
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | birth_date |
      | 1001         | 1980-06-15 |
      | 1002         | 1985-11-22 |
      | 1003         | 1975-03-08 |

  Scenario: Validate address information mapping
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | address_city | address_state | address_country |
      | 1001         | New York     | NY            | USA             |
      | 1002         | Los Angeles  | CA            | USA             |
      | 1003         | Chicago      | IL            | USA             |

  Scenario: Validate sales aggregation calculations
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | total_orders | total_sales_amount | total_profit | total_items_purchased |
      | 1001         | 2            | 225.00             | 45.00        | 3                     |
      | 1002         | 1            | 225.00             | 45.00        | 3                     |
      | 1003         | 1            | 100.00             | 20.00        | 1                     |

  Scenario: Validate date calculations
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | first_purchase_date | last_purchase_date |
      | 1001         | 2023-01-15          | 2023-02-20         |
      | 1002         | 2023-03-10          | 2023-03-10         |
      | 1003         | 2023-04-05          | 2023-04-05         |

  Scenario: Validate average order value calculation
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | avg_order_value |
      | 1001         | 112.50          |
      | 1002         | 225.00          |
      | 1003         | 100.00          |

  Scenario: Validate favorite category and brand determination
    When the ETL pipeline processes the data
    Then the target table customer_sales_analytics should contain:
      | customer_key | favorite_category | favorite_brand |
      | 1001         | Electronics       | BrandA         |
      | 1002         | Electronics       | BrandA         |
      | 1003         | Electronics       | BrandA         |

  Scenario: Validate join relationships integrity
    Given web_sales record with ws_bill_customer_sk that does not exist in customer table
    When the ETL pipeline processes the data
    Then that web_sales record should be excluded from the result set

  Scenario: Validate null value handling
    Given customer record with null c_email_address
    When the ETL pipeline processes the data
    Then the corresponding record in customer_sales_analytics should have null email

  Scenario: Validate empty sales data for customer
    Given customer record with no corresponding web_sales records
    When the ETL pipeline processes the data
    Then the customer should appear in customer_sales_analytics with:
      | total_orders | total_sales_amount | total_profit | total_items_purchased | avg_order_value |
      | 0            | 0.00               | 0.00         | 0                     | null            |

  Scenario: Validate data type conversions
    When the ETL pipeline processes the data
    Then customer_key should be of type NUMBER
    And customer_id should be of type STRING
    And birth_date should be of type DATE
    And total_sales_amount should be of type NUMBER with 2 decimal places

  Scenario: Validate record count consistency
    Given 1000 customer records in source
    When the ETL pipeline processes the data
    Then customer_sales_analytics should contain exactly 1000 records