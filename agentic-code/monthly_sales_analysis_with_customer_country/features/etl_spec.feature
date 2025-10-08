# Referenced similar ticket SCRUM-9: Monthly sales analysis data product pattern
# Referenced similar ticket SCRUM-7: Customer region dimension approach
# This specification extends monthly sales analysis by adding customer country dimension

Feature: monthly_sales_analysis_with_customer_country

  Background: Source and Target Schema Definition
    Given the following source tables exist:
      | table_name        | columns |
      | store_sales       | ss_sold_date_sk:INTEGER, ss_customer_sk:INTEGER, ss_item_sk:INTEGER, ss_store_sk:INTEGER, ss_ext_sales_price:NUMBER, ss_quantity:INTEGER, ss_net_paid:NUMBER, ss_net_profit:NUMBER |
      | customer          | c_customer_sk:INTEGER, c_current_addr_sk:INTEGER, c_birth_country:STRING, c_first_name:STRING, c_last_name:STRING |
      | customer_address  | ca_address_sk:INTEGER, ca_country:STRING, ca_state:STRING, ca_city:STRING |
      | date_dim          | d_date_sk:INTEGER, d_date:DATE, d_year:INTEGER, d_moy:INTEGER, d_month_seq:INTEGER |
      | item              | i_item_sk:INTEGER, i_item_id:STRING, i_item_desc:STRING, i_category:STRING |
      | store             | s_store_sk:INTEGER, s_store_id:STRING, s_store_name:STRING |

    And the following target table schema:
      | table_name                      | columns |
      | monthly_sales_analysis_country  | year:INTEGER, month:INTEGER, month_seq:INTEGER, customer_sk:INTEGER, customer_country:STRING, customer_birth_country:STRING, item_sk:INTEGER, store_sk:INTEGER, total_sales_amount:NUMBER, total_quantity:INTEGER, total_net_paid:NUMBER, total_net_profit:NUMBER, transaction_count:INTEGER |

  Scenario: Extract monthly sales data with customer country dimension
    Given sales transactions exist in store_sales table
    And customers have address information in customer and customer_address tables
    And date information is available in date_dim table
    
    When extracting monthly sales data with customer country information
    
    Then join store_sales to customer on customer surrogate key to get customer details
    And join customer to customer_address on current address surrogate key to get country information
    And join store_sales to date_dim on sold date surrogate key to get year and month information
    And join store_sales to item on item surrogate key to get item details
    And join store_sales to store on store surrogate key to get store details
    
    And aggregate sales data by year, month, customer, customer country, item, and store
    And calculate total sales amount as sum of extended sales price
    And calculate total quantity as sum of quantity sold
    And calculate total net paid as sum of net paid amount
    And calculate total net profit as sum of net profit
    And calculate transaction count as count of distinct transactions
    And include customer birth country from customer table as additional country dimension
    And include current address country from customer address table as primary country dimension
    
    And filter out records where customer country is null or empty
    And order results by year, month sequence, and total sales amount descending