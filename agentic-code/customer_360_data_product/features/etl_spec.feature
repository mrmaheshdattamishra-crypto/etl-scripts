Feature: customer_360_data_product
  As a business analyst
  I want to create a comprehensive customer 360 view
  So that I can analyze customer behavior across all channels

  Background:
    Given the following source tables exist:
      | table_name           | columns                                                                                      |
      | customer            | c_customer_sk (NUMBER), c_customer_id (STRING), c_current_addr_sk (NUMBER), c_first_name (STRING), c_last_name (STRING), c_email_address (STRING), c_birth_year (NUMBER), c_preferred_cust_flag (STRING) |
      | customer_address    | ca_address_sk (NUMBER), ca_city (STRING), ca_county (STRING), ca_state (STRING), ca_zip (STRING), ca_country (STRING) |
      | customer_demographics | cd_demo_sk (NUMBER), cd_gender (STRING), cd_marital_status (STRING), cd_education_status (STRING), cd_purchase_estimate (NUMBER) |
      | catalog_sales       | cs_bill_customer_sk (NUMBER), cs_sold_date_sk (NUMBER), cs_item_sk (NUMBER), cs_quantity (NUMBER), cs_sales_price (NUMBER), cs_net_profit (NUMBER) |
      | date_dim           | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_quarter_name (STRING), d_day_name (STRING) |

    And the target table schema is:
      | table_name     | columns                                                                                      |
      | customer_360   | customer_key (NUMBER), customer_id (STRING), full_name (STRING), email (STRING), birth_year (NUMBER), preferred_flag (STRING), address_city (STRING), address_state (STRING), address_country (STRING), gender (STRING), marital_status (STRING), education (STRING), purchase_estimate (NUMBER), total_orders (NUMBER), total_quantity (NUMBER), total_sales_amount (NUMBER), total_profit (NUMBER), first_purchase_date (DATE), last_purchase_date (DATE), avg_order_value (NUMBER) |

  Scenario: Join customer with address and demographics
    Given customer table contains customer basic information
    And customer_address table contains address details
    And customer_demographics table contains demographic information
    When I join customer to customer_address on current address surrogate key
    And I join customer to customer_demographics on current demographics surrogate key
    Then I combine customer first name and last name with space separator to create full name
    And I map customer city, state and country from address table
    And I map gender, marital status and education from demographics table

  Scenario: Aggregate sales data by customer
    Given catalog_sales table contains transaction data
    And date_dim table contains date information
    When I join catalog_sales to date_dim on sold date surrogate key
    And I group sales data by bill customer surrogate key
    Then I count distinct order numbers to calculate total orders
    And I sum quantity to calculate total quantity purchased
    And I sum sales price to calculate total sales amount
    And I sum net profit to calculate total profit
    And I find minimum date for first purchase date
    And I find maximum date for last purchase date
    And I divide total sales amount by total orders to calculate average order value

  Scenario: Create final customer 360 view
    Given customer profile data is prepared
    And customer sales aggregation data is prepared
    When I join customer profile to sales aggregation on customer surrogate key
    Then I create customer_360 table with all customer attributes and sales metrics
    And I handle null values for customers with no sales history by setting sales metrics to zero