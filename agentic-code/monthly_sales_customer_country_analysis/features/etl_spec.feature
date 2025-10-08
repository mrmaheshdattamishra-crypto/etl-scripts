# Referenced similar tickets: SCRUM-9 (monthly sales analytics), SCRUM-7 (customer region dimension)
# Building upon existing monthly sales analysis pattern with customer country dimension enhancement

Feature: monthly_sales_customer_country_analysis
  As a business analyst
  I want to analyze monthly sales by customer country
  So that I can understand sales patterns across different countries

  Background:
    Given source schema contains:
      | table_name        | column_name         | data_type |
      | store_sales       | ss_sold_date_sk     | NUMBER    |
      | store_sales       | ss_customer_sk      | NUMBER    |
      | store_sales       | ss_ext_sales_price  | NUMBER    |
      | store_sales       | ss_quantity         | NUMBER    |
      | store_sales       | ss_net_profit       | NUMBER    |
      | customer          | c_customer_sk       | NUMBER    |
      | customer          | c_current_addr_sk   | NUMBER    |
      | customer          | c_birth_country     | STRING    |
      | customer_address  | ca_address_sk       | NUMBER    |
      | customer_address  | ca_country          | STRING    |
      | date_dim          | d_date_sk           | NUMBER    |
      | date_dim          | d_year              | NUMBER    |
      | date_dim          | d_moy               | NUMBER    |
      | date_dim          | d_date              | DATE      |
    
    And target schema contains:
      | table_name                    | column_name           | data_type |
      | monthly_sales_by_country      | year_month            | STRING    |
      | monthly_sales_by_country      | customer_country      | STRING    |
      | monthly_sales_by_country      | birth_country         | STRING    |
      | monthly_sales_by_country      | total_sales_amount    | NUMBER    |
      | monthly_sales_by_country      | total_quantity        | NUMBER    |
      | monthly_sales_by_country      | total_profit          | NUMBER    |
      | monthly_sales_by_country      | unique_customers      | NUMBER    |
      | monthly_sales_by_country      | avg_sales_per_customer| NUMBER    |

  Scenario: Transform sales data with customer country dimension
    Given I have sales transactions from store_sales table
    And I need to join with customer information for country data
    And I need to aggregate by year-month and country
    
    When I join store_sales with customer on customer surrogate key
    And I join customer with customer_address on address surrogate key  
    And I join store_sales with date_dim on sold date surrogate key
    And I filter for valid sales transactions with positive sales amounts
    And I group by year-month and customer countries
    
    Then I calculate total sales amount as sum of extended sales price
    And I calculate total quantity as sum of quantity sold
    And I calculate total profit as sum of net profit
    And I calculate unique customers as count of distinct customer keys
    And I calculate average sales per customer as total sales divided by unique customers
    And I create year-month field by concatenating year and month with hyphen
    And I use customer address country as primary country dimension
    And I include customer birth country as additional country attribute
    And I handle null country values by setting them to 'Unknown'
    
    And the result contains monthly sales aggregated by customer country
    And each row represents one year-month and country combination
    And all monetary amounts are rounded to two decimal places