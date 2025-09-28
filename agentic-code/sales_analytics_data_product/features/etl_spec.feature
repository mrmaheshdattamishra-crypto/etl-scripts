Feature: sales_analytics_data_product

Background:
  Given the following source table schemas exist:
    | table_name          | columns                                                                                                    |
    | web_sales          | ws_sold_date_sk (NUMBER), ws_item_sk (NUMBER), ws_bill_customer_sk (NUMBER), ws_quantity (NUMBER), ws_sales_price (NUMBER), ws_ext_sales_price (NUMBER), ws_net_profit (NUMBER), ws_order_number (NUMBER) |
    | store_sales        | ss_sold_date_sk (NUMBER), ss_item_sk (NUMBER), ss_customer_sk (NUMBER), ss_quantity (NUMBER), ss_sales_price (NUMBER), ss_ext_sales_price (NUMBER), ss_net_profit (NUMBER), ss_ticket_number (NUMBER) |
    | catalog_sales      | cs_sold_date_sk (NUMBER), cs_item_sk (NUMBER), cs_bill_customer_sk (NUMBER), cs_quantity (NUMBER), cs_sales_price (NUMBER), cs_ext_sales_price (NUMBER), cs_net_profit (NUMBER), cs_order_number (NUMBER) |
    | date_dim           | d_date_sk (NUMBER), d_date (DATE), d_year (NUMBER), d_month_seq (NUMBER), d_quarter_seq (NUMBER), d_day_name (STRING), d_quarter_name (STRING) |
    | item               | i_item_sk (NUMBER), i_item_id (STRING), i_item_desc (STRING), i_brand (STRING), i_category (STRING), i_class (STRING), i_current_price (NUMBER) |
    | customer           | c_customer_sk (NUMBER), c_customer_id (STRING), c_first_name (STRING), c_last_name (STRING), c_birth_year (NUMBER), c_current_addr_sk (NUMBER) |
    | customer_address   | ca_address_sk (NUMBER), ca_city (STRING), ca_county (STRING), ca_state (STRING), ca_zip (STRING), ca_country (STRING) |
    | store              | s_store_sk (NUMBER), s_store_id (STRING), s_store_name (STRING), s_city (STRING), s_state (STRING), s_company_name (STRING) |

  And the target table schema is:
    | table_name         | columns                                                                                                    |
    | sales_fact         | sale_id (STRING), sale_date (DATE), sale_year (NUMBER), sale_quarter (STRING), channel (STRING), customer_id (STRING), customer_name (STRING), customer_city (STRING), customer_state (STRING), item_id (STRING), item_description (STRING), item_brand (STRING), item_category (STRING), store_id (STRING), store_name (STRING), store_city (STRING), quantity (NUMBER), unit_price (NUMBER), total_sales (NUMBER), profit (NUMBER) |

Scenario: Transform multi-channel sales data into unified sales analytics fact table
  Given I have access to web sales, store sales, and catalog sales data
  And I have dimensional data for dates, items, customers, addresses, and stores
  
  When I join web_sales with date_dim on ws_sold_date_sk equals d_date_sk
  And I join web_sales with item on ws_item_sk equals i_item_sk  
  And I join web_sales with customer on ws_bill_customer_sk equals c_customer_sk
  And I join customer with customer_address on c_current_addr_sk equals ca_address_sk
  
  And I join store_sales with date_dim on ss_sold_date_sk equals d_date_sk
  And I join store_sales with item on ss_item_sk equals i_item_sk
  And I join store_sales with customer on ss_customer_sk equals c_customer_sk  
  And I join customer with customer_address on c_current_addr_sk equals ca_address_sk
  And I join store_sales with store on ss_store_sk equals s_store_sk
  
  And I join catalog_sales with date_dim on cs_sold_date_sk equals d_date_sk
  And I join catalog_sales with item on cs_item_sk equals i_item_sk
  And I join catalog_sales with customer on cs_bill_customer_sk equals c_customer_sk
  And I join customer with customer_address on c_current_addr_sk equals ca_address_sk

  Then I map the following fields for web sales records:
    | target_field       | source_mapping                                                           |
    | sale_id           | concatenate "WS" with ws_order_number                                   |
    | sale_date         | d_date from date_dim                                                    |
    | sale_year         | d_year from date_dim                                                    |
    | sale_quarter      | d_quarter_name from date_dim                                            |
    | channel           | set to "Web"                                                            |
    | customer_id       | c_customer_id from customer                                             |
    | customer_name     | concatenate c_first_name and c_last_name from customer                  |
    | customer_city     | ca_city from customer_address                                           |
    | customer_state    | ca_state from customer_address                                          |
    | item_id           | i_item_id from item                                                     |
    | item_description  | i_item_desc from item                                                   |
    | item_brand        | i_brand from item                                                       |
    | item_category     | i_category from item                                                    |
    | store_id          | set to null                                                             |
    | store_name        | set to null                                                             |
    | store_city        | set to null                                                             |
    | quantity          | ws_quantity from web_sales                                              |
    | unit_price        | ws_sales_price from web_sales                                           |
    | total_sales       | ws_ext_sales_price from web_sales                                       |
    | profit            | ws_net_profit from web_sales                                            |

  And I map the following fields for store sales records:
    | target_field       | source_mapping                                                           |
    | sale_id           | concatenate "SS" with ss_ticket_number                                  |
    | sale_date         | d_date from date_dim                                                    |
    | sale_year         | d_year from date_dim                                                    |
    | sale_quarter      | d_quarter_name from date_dim                                            |
    | channel           | set to "Store"                                                          |
    | customer_id       | c_customer_id from customer                                             |
    | customer_name     | concatenate c_first_name and c_last_name from customer                  |
    | customer_city     | ca_city from customer_address                                           |
    | customer_state    | ca_state from customer_address                                          |
    | item_id           | i_item_id from item                                                     |
    | item_description  | i_item_desc from item                                                   |
    | item_brand        | i_brand from item                                                       |
    | item_category     | i_category from item                                                    |
    | store_id          | s_store_id from store                                                   |
    | store_name        | s_store_name from store                                                 |
    | store_city        | s_city from store                                                       |
    | quantity          | ss_quantity from store_sales                                            |
    | unit_price        | ss_sales_price from store_sales                                         |
    | total_sales       | ss_ext_sales_price from store_sales                                     |
    | profit            | ss_net_profit from store_sales                                          |

  And I map the following fields for catalog sales records:
    | target_field       | source_mapping                                                           |
    | sale_id           | concatenate "CS" with cs_order_number                                   |
    | sale_date         | d_date from date_dim                                                    |
    | sale_year         | d_year from date_dim                                                    |
    | sale_quarter      | d_quarter_name from date_dim                                            |
    | channel           | set to "Catalog"                                                        |
    | customer_id       | c_customer_id from customer                                             |
    | customer_name     | concatenate c_first_name and c_last_name from customer                  |
    | customer_city     | ca_city from customer_address                                           |
    | customer_state    | ca_state from customer_address                                          |
    | item_id           | i_item_id from item                                                     |
    | item_description  | i_item_desc from item                                                   |
    | item_brand        | i_brand from item                                                       |
    | item_category     | i_category from item                                                    |
    | store_id          | set to null                                                             |
    | store_name        | set to null                                                             |
    | store_city        | set to null                                                             |
    | quantity          | cs_quantity from catalog_sales                                          |
    | unit_price        | cs_sales_price from catalog_sales                                       |
    | total_sales       | cs_ext_sales_price from catalog_sales                                   |
    | profit            | cs_net_profit from catalog_sales                                        |

  And I union all three transformed datasets into the target sales_fact table
  And I filter out records where sale_date is null or quantity is less than or equal to zero