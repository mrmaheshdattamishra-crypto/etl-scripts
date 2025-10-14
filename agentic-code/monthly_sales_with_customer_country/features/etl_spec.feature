# Referenced similar ticket SCRUM-9: CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT
# Referenced similar ticket SCRUM-7: Customer by Region - adapting country dimension pattern
# Building upon existing monthly sales analysis to add customer country dimension

Feature: monthly_sales_with_customer_country

Scenario: Create monthly sales analysis with customer country dimension
  Given source table policy_customers with schema:
    | Column Name          | Data Type |
    | policy_id           | STRING    |
    | c_id                | STRING    |
    | customer_name       | STRING    |
    | email               | STRING    |
    | phone               | STRING    |
    | address             | STRING    |
    | policy_type         | STRING    |
    | policy_start_date   | STRING    |
    | policy_end_date     | STRING    |
    | policy_term_months  | NUMBER    |
    | annual_premium_usd  | NUMBER    |
    | agent_id            | STRING    |
    | multi_policy_discount | STRING  |
    | safe_driver_discount  | STRING  |
    | state               | STRING    |

  And source table financial_transactions with schema:
    | Column Name      | Data Type |
    | transaction_id   | STRING    |
    | policy_id        | STRING    |
    | date             | DATE      |
    | type             | STRING    |
    | amount_usd       | NUMBER    |
    | payment_method   | STRING    |
    | reconciled       | STRING    |

  When I join policy_customers and financial_transactions on policy_id
  And I create target table dim_customer_country with schema:
    | Column Name      | Data Type |
    | customer_id      | STRING    |
    | customer_name    | STRING    |
    | state            | STRING    |
    | country          | STRING    |
    | country_region   | STRING    |

  And I create target table fact_monthly_sales_country with schema:
    | Column Name      | Data Type |
    | sales_month      | DATE      |
    | customer_id      | STRING    |
    | country          | STRING    |
    | country_region   | STRING    |
    | policy_type      | STRING    |
    | total_sales      | NUMBER    |
    | transaction_count| NUMBER    |
    | avg_transaction  | NUMBER    |

  Then I map data as follows:
    For dim_customer_country:
      customer_id maps to c_id from policy_customers
      customer_name maps to customer_name from policy_customers
      state maps to state from policy_customers
      country maps to derived value "USA" for all records
      country_region maps to derived value based on state groupings where Northeast includes states like NY MA CT, Southeast includes states like FL GA SC, Midwest includes states like IL OH MI, Southwest includes states like TX AZ NM, West includes states like CA WA OR, and others map to "Other"

    For fact_monthly_sales_country:
      sales_month maps to date truncated to month from financial_transactions
      customer_id maps to c_id from policy_customers
      country maps to derived value "USA" for all records
      country_region maps to same logic as dim_customer_country
      policy_type maps to policy_type from policy_customers
      total_sales maps to sum of amount_usd from financial_transactions grouped by month and customer
      transaction_count maps to count of transactions grouped by month and customer
      avg_transaction maps to average of amount_usd from financial_transactions grouped by month and customer