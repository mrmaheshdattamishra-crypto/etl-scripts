# Referenced similar tickets: SCRUM-9 (monthly sales analysis), SCRUM-7 (customer region extension)
# This specification extends existing monthly sales analysis with customer country dimension

Feature: monthly_sales_analysis_with_country_dimension
  As a business analyst
  I want to analyze monthly sales data with customer country dimension
  So that I can understand sales performance across different countries

  Background:
    Given the following source tables exist:
      | Table Name        | Columns                                                          | Data Types                                  |
      | store_sales       | ss_customer_sk, ss_sold_date_sk, ss_ext_sales_price, ss_quantity | NUMBER, NUMBER, NUMBER, NUMBER              |
      | customer          | c_customer_sk, c_current_addr_sk, c_birth_country               | NUMBER, NUMBER, STRING                      |
      | customer_address  | ca_address_sk, ca_country                                        | NUMBER, STRING                              |
      | date_dim          | d_date_sk, d_year, d_moy, d_date                                | NUMBER, NUMBER, NUMBER, DATE                |

  Scenario: Create customer country dimension table
    Given I have access to customer and customer_address tables
    When I create the customer_country_dim table
    Then the target table should have the following schema:
      | Column Name           | Data Type | Description                    |
      | customer_country_key  | NUMBER    | Surrogate key for dimension    |
      | customer_sk          | NUMBER    | Customer surrogate key         |
      | birth_country        | STRING    | Customer birth country         |
      | address_country      | STRING    | Customer address country       |
      | primary_country      | STRING    | Primary country for customer   |
      | country_region       | STRING    | Geographic region              |
    And the mapping logic should be:
      - customer_country_key is generated as sequential surrogate key
      - customer_sk maps from customer.c_customer_sk
      - birth_country maps from customer.c_birth_country
      - address_country maps from customer_address.ca_country joined on customer.c_current_addr_sk equals customer_address.ca_address_sk
      - primary_country is coalesce of address_country and birth_country with address_country taking precedence
      - country_region is derived by mapping countries to their respective geographic regions

  Scenario: Create monthly sales analysis with country dimension
    Given I have access to store_sales, date_dim, customer, and customer_address tables
    When I create the monthly_sales_by_country table
    Then the target table should have the following schema:
      | Column Name          | Data Type | Description                           |
      | sales_year           | NUMBER    | Year of sales                         |
      | sales_month          | NUMBER    | Month of sales                        |
      | customer_country     | STRING    | Customer primary country              |
      | country_region       | STRING    | Geographic region                     |
      | total_sales_amount   | NUMBER    | Sum of extended sales price           |
      | total_quantity       | NUMBER    | Sum of quantity sold                  |
      | unique_customers     | NUMBER    | Count of distinct customers           |
      | avg_sales_per_customer| NUMBER   | Average sales amount per customer     |
    And the join relationships should be:
      - store_sales joins with date_dim on ss_sold_date_sk equals d_date_sk
      - store_sales joins with customer on ss_customer_sk equals c_customer_sk
      - customer joins with customer_address on c_current_addr_sk equals ca_address_sk
    And the mapping logic should be:
      - sales_year maps from date_dim.d_year
      - sales_month maps from date_dim.d_moy
      - customer_country is coalesce of customer_address.ca_country and customer.c_birth_country with ca_country taking precedence
      - country_region is derived by mapping customer_country to geographic regions
      - total_sales_amount is sum of store_sales.ss_ext_sales_price grouped by year, month, and country
      - total_quantity is sum of store_sales.ss_quantity grouped by year, month, and country
      - unique_customers is count distinct of store_sales.ss_customer_sk grouped by year, month, and country
      - avg_sales_per_customer is total_sales_amount divided by unique_customers