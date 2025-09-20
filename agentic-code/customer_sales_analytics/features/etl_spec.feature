Feature: customer_sales_analytics

  Background: Schema specification
    Given source tables:
      | table_name         | columns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
      | customer           | c_customer_sk: NUMBER, c_customer_id: STRING, c_current_cdemo_sk: NUMBER, c_current_hdemo_sk: NUMBER, c_current_addr_sk: NUMBER, c_first_shipto_date_sk: NUMBER, c_first_sales_date_sk: NUMBER, c_salutation: STRING, c_first_name: STRING, c_last_name: STRING, c_preferred_cust_flag: STRING, c_birth_day: NUMBER, c_birth_month: NUMBER, c_birth_year: NUMBER, c_birth_country: STRING, c_login: STRING, c_email_address: STRING, c_last_review_date_sk: NUMBER                                                                                                                                                                                                                                                                                          |
      | web_sales          | ws_sold_date_sk: NUMBER, ws_sold_time_sk: NUMBER, ws_ship_date_sk: NUMBER, ws_item_sk: NUMBER, ws_bill_customer_sk: NUMBER, ws_bill_cdemo_sk: NUMBER, ws_bill_hdemo_sk: NUMBER, ws_bill_addr_sk: NUMBER, ws_ship_customer_sk: NUMBER, ws_ship_cdemo_sk: NUMBER, ws_ship_hdemo_sk: NUMBER, ws_ship_addr_sk: NUMBER, ws_web_page_sk: NUMBER, ws_web_site_sk: NUMBER, ws_ship_mode_sk: NUMBER, ws_warehouse_sk: NUMBER, ws_promo_sk: NUMBER, ws_order_number: NUMBER, ws_quantity: NUMBER, ws_wholesale_cost: NUMBER, ws_list_price: NUMBER, ws_sales_price: NUMBER, ws_ext_discount_amt: NUMBER, ws_ext_sales_price: NUMBER, ws_ext_wholesale_cost: NUMBER, ws_ext_list_price: NUMBER, ws_ext_tax: NUMBER, ws_coupon_amt: NUMBER, ws_ext_ship_cost: NUMBER, ws_net_paid: NUMBER, ws_net_paid_inc_tax: NUMBER, ws_net_paid_inc_ship: NUMBER, ws_net_paid_inc_ship_tax: NUMBER, ws_net_profit: NUMBER |
      | date_dim           | d_date_sk: NUMBER, d_date_id: STRING, d_date: DATE, d_month_seq: NUMBER, d_week_seq: NUMBER, d_quarter_seq: NUMBER, d_year: NUMBER, d_dow: NUMBER, d_moy: NUMBER, d_dom: NUMBER, d_qoy: NUMBER, d_fy_year: NUMBER, d_fy_quarter_seq: NUMBER, d_fy_week_seq: NUMBER, d_day_name: STRING, d_quarter_name: STRING, d_holiday: STRING, d_weekend: STRING, d_following_holiday: STRING, d_first_dom: NUMBER, d_last_dom: NUMBER, d_same_day_ly: NUMBER, d_same_day_lq: NUMBER, d_current_day: STRING, d_current_week: STRING, d_current_month: STRING, d_current_quarter: STRING, d_current_year: STRING                                                                                                                                                                                                                                                    |
      | item               | i_item_sk: NUMBER, i_item_id: STRING, i_rec_start_date: DATE, i_rec_end_date: DATE, i_item_desc: STRING, i_current_price: NUMBER, i_wholesale_cost: NUMBER, i_brand_id: NUMBER, i_brand: STRING, i_class_id: NUMBER, i_class: STRING, i_category_id: NUMBER, i_category: STRING, i_manufact_id: NUMBER, i_manufact: STRING, i_size: STRING, i_formulation: STRING, i_color: STRING, i_units: STRING, i_container: STRING, i_manager_id: NUMBER, i_product_name: STRING                                                                                                                                                                                                                                                                                                                                             |
      | customer_address   | ca_address_sk: NUMBER, ca_address_id: STRING, ca_street_number: STRING, ca_street_name: STRING, ca_street_type: STRING, ca_suite_number: STRING, ca_city: STRING, ca_county: STRING, ca_state: STRING, ca_zip: STRING, ca_country: STRING, ca_gmt_offset: NUMBER, ca_location_type: STRING                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
    
    And target table:
      | table_name                | columns                                                                                                                                                                                                                                                                                                                                                                                                                                |
      | customer_sales_analytics  | customer_key: NUMBER, customer_id: STRING, full_name: STRING, birth_date: DATE, email: STRING, preferred_customer: STRING, address_city: STRING, address_state: STRING, address_country: STRING, total_orders: NUMBER, total_sales_amount: NUMBER, total_profit: NUMBER, first_purchase_date: DATE, last_purchase_date: DATE, avg_order_value: NUMBER, total_items_purchased: NUMBER, favorite_category: STRING, favorite_brand: STRING |

  Scenario: Create customer sales analytics data mart
    Given web sales data joined with customer data
    And sales data joined with date dimension for temporal analysis
    And sales data joined with item data for product insights
    And customer data joined with address data for geographic analysis
    
    When transforming data:
      Then map customer_key from c_customer_sk
      And map customer_id from c_customer_id
      And map full_name by concatenating c_first_name and c_last_name with space separator
      And map birth_date by combining c_birth_year, c_birth_month, and c_birth_day into date format
      And map email from c_email_address
      And map preferred_customer from c_preferred_cust_flag
      And map address_city from ca_city
      And map address_state from ca_state  
      And map address_country from ca_country
      And calculate total_orders by counting distinct ws_order_number per customer
      And calculate total_sales_amount by summing ws_ext_sales_price per customer
      And calculate total_profit by summing ws_net_profit per customer
      And calculate first_purchase_date by finding minimum d_date for each customer
      And calculate last_purchase_date by finding maximum d_date for each customer
      And calculate avg_order_value by dividing total_sales_amount by total_orders
      And calculate total_items_purchased by summing ws_quantity per customer
      And determine favorite_category by finding most frequently purchased i_category per customer
      And determine favorite_brand by finding most frequently purchased i_brand per customer

    And join relationships are:
      | left_table        | left_key            | right_table       | right_key         |
      | web_sales         | ws_bill_customer_sk | customer          | c_customer_sk     |
      | web_sales         | ws_sold_date_sk     | date_dim          | d_date_sk         |
      | web_sales         | ws_item_sk          | item              | i_item_sk         |
      | customer          | c_current_addr_sk   | customer_address  | ca_address_sk     |