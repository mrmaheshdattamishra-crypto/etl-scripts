Feature: Sales Analytics Data Product
  As a business analyst
  I want to create a unified sales analytics data model
  So that I can analyze sales performance across all channels

Background:
  Given the data product name is "sales_analytics"

Scenario: Define source table schemas
  Given the following source tables exist:
    | table_name      | columns                                                                                              | datatypes                                                                |
    | store_sales     | ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_store_sk, ss_quantity, ss_sales_price, ss_net_profit | NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER                  |
    | catalog_sales   | cs_sold_date_sk, cs_item_sk, cs_bill_customer_sk, cs_quantity, cs_sales_price, cs_net_profit        | NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER                          |
    | web_sales       | ws_sold_date_sk, ws_item_sk, ws_bill_customer_sk, ws_quantity, ws_sales_price, ws_net_profit        | NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER                          |
    | store           | s_store_sk, s_store_id, s_store_name, s_state, s_city                                               | NUMBER, STRING, STRING, STRING, STRING                                  |
    | customer        | c_customer_sk, c_customer_id, c_first_name, c_last_name, c_birth_year                              | NUMBER, STRING, STRING, STRING, NUMBER                                  |
    | item            | i_item_sk, i_item_id, i_item_desc, i_category, i_brand                                              | NUMBER, STRING, STRING, STRING, STRING                                  |
    | date_dim        | d_date_sk, d_date, d_year, d_month_seq, d_quarter, d_month                                          | NUMBER, DATE, NUMBER, NUMBER, NUMBER, NUMBER                            |

Scenario: Define target data model schema
  Given the target table "sales_fact" has the following schema:
    | column_name        | datatype | description                    |
    | sale_id            | STRING   | unique identifier for each sale |
    | sale_date          | DATE     | date of the sale               |
    | customer_id        | STRING   | customer identifier            |
    | item_id            | STRING   | item identifier                |
    | store_id           | STRING   | store identifier               |
    | channel            | STRING   | sales channel                  |
    | quantity           | NUMBER   | quantity sold                  |
    | sales_amount       | NUMBER   | total sales amount             |
    | profit_amount      | NUMBER   | net profit amount              |
    | year               | NUMBER   | sale year                      |
    | quarter            | NUMBER   | sale quarter                   |
    | month              | NUMBER   | sale month                     |
    | customer_name      | STRING   | full customer name             |
    | item_description   | STRING   | item description               |
    | item_category      | STRING   | item category                  |
    | store_name         | STRING   | store name                     |
    | store_location     | STRING   | store city and state           |

Scenario: Define table join relationships
  Given the following join relationships exist:
    | left_table     | left_key           | right_table | right_key      | join_type |
    | store_sales    | ss_sold_date_sk    | date_dim    | d_date_sk      | inner     |
    | store_sales    | ss_customer_sk     | customer    | c_customer_sk  | left      |
    | store_sales    | ss_item_sk         | item        | i_item_sk      | left      |
    | store_sales    | ss_store_sk        | store       | s_store_sk     | left      |
    | catalog_sales  | cs_sold_date_sk    | date_dim    | d_date_sk      | inner     |
    | catalog_sales  | cs_bill_customer_sk| customer    | c_customer_sk  | left      |
    | catalog_sales  | cs_item_sk         | item        | i_item_sk      | left      |
    | web_sales      | ws_sold_date_sk    | date_dim    | d_date_sk      | inner     |
    | web_sales      | ws_bill_customer_sk| customer    | c_customer_sk  | left      |
    | web_sales      | ws_item_sk         | item        | i_item_sk      | left      |

Scenario: Map store sales data to target model
  When transforming store_sales data
  Then map the following fields:
    | source_field       | target_field       | transformation_logic                                           |
    | ss_ticket_number   | sale_id            | concatenate channel prefix 'STORE_' with ticket number        |
    | d_date             | sale_date          | use date from date dimension                                   |
    | c_customer_id      | customer_id        | use customer id from customer table                            |
    | i_item_id          | item_id            | use item id from item table                                    |
    | s_store_id         | store_id           | use store id from store table                                  |
    | literal            | channel            | set constant value 'STORE'                                     |
    | ss_quantity        | quantity           | use store sales quantity                                       |
    | ss_sales_price     | sales_amount       | use store sales price                                          |
    | ss_net_profit      | profit_amount      | use store net profit                                           |
    | d_year             | year               | use year from date dimension                                   |
    | d_quarter          | quarter            | use quarter from date dimension                                |
    | d_month            | month              | use month from date dimension                                  |
    | c_first_name       | customer_name      | concatenate first name and last name with space               |
    | i_item_desc        | item_description   | use item description                                           |
    | i_category         | item_category      | use item category                                              |
    | s_store_name       | store_name         | use store name                                                 |
    | s_city             | store_location     | concatenate city comma space and state                         |

Scenario: Map catalog sales data to target model
  When transforming catalog_sales data
  Then map the following fields:
    | source_field         | target_field       | transformation_logic                                           |
    | cs_order_number      | sale_id            | concatenate channel prefix 'CATALOG_' with order number       |
    | d_date               | sale_date          | use date from date dimension                                   |
    | c_customer_id        | customer_id        | use customer id from customer table                            |
    | i_item_id            | item_id            | use item id from item table                                    |
    | literal              | store_id           | set null value for catalog sales                               |
    | literal              | channel            | set constant value 'CATALOG'                                   |
    | cs_quantity          | quantity           | use catalog sales quantity                                     |
    | cs_sales_price       | sales_amount       | use catalog sales price                                        |
    | cs_net_profit        | profit_amount      | use catalog net profit                                         |
    | d_year               | year               | use year from date dimension                                   |
    | d_quarter            | quarter            | use quarter from date dimension                                |
    | d_month              | month              | use month from date dimension                                  |
    | c_first_name         | customer_name      | concatenate first name and last name with space               |
    | i_item_desc          | item_description   | use item description                                           |
    | i_category           | item_category      | use item category                                              |
    | literal              | store_name         | set null value for catalog sales                               |
    | literal              | store_location     | set null value for catalog sales                               |

Scenario: Map web sales data to target model
  When transforming web_sales data
  Then map the following fields:
    | source_field         | target_field       | transformation_logic                                           |
    | ws_order_number      | sale_id            | concatenate channel prefix 'WEB_' with order number           |
    | d_date               | sale_date          | use date from date dimension                                   |
    | c_customer_id        | customer_id        | use customer id from customer table                            |
    | i_item_id            | item_id            | use item id from item table                                    |
    | literal              | store_id           | set null value for web sales                                   |
    | literal              | channel            | set constant value 'WEB'                                       |
    | ws_quantity          | quantity           | use web sales quantity                                         |
    | ws_sales_price       | sales_amount       | use web sales price                                            |
    | ws_net_profit        | profit_amount      | use web net profit                                             |
    | d_year               | year               | use year from date dimension                                   |
    | d_quarter            | quarter            | use quarter from date dimension                                |
    | d_month              | month              | use month from date dimension                                  |
    | c_first_name         | customer_name      | concatenate first name and last name with space               |
    | i_item_desc          | item_description   | use item description                                           |
    | i_category           | item_category      | use item category                                              |
    | literal              | store_name         | set null value for web sales                                   |
    | literal              | store_location     | set null value for web sales                                   |

Scenario: Combine all sales channels
  When creating the final sales_fact table
  Then union all transformed data from store sales, catalog sales, and web sales
  And apply data quality filters to exclude records with null sale dates or negative quantities
  And sort the final dataset by sale date and channel