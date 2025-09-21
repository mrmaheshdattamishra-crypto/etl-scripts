Feature: monthly_sales_star_schema

Background:
  Given source schema contains tables:
    | table_name   | columns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
    | store_sales  | ss_sold_date_sk NUMBER, ss_item_sk NUMBER, ss_store_sk NUMBER, ss_quantity NUMBER, ss_wholesale_cost NUMBER, ss_list_price NUMBER, ss_sales_price NUMBER, ss_ext_discount_amt NUMBER, ss_ext_sales_price NUMBER, ss_ext_wholesale_cost NUMBER, ss_ext_list_price NUMBER, ss_ext_tax NUMBER, ss_coupon_amt NUMBER, ss_net_paid NUMBER, ss_net_paid_inc_tax NUMBER, ss_net_profit NUMBER                                                                                                                                                                                                                                                                                                                                                |
    | item         | i_item_sk NUMBER, i_item_id STRING, i_item_desc STRING, i_current_price NUMBER, i_wholesale_cost NUMBER, i_brand_id NUMBER, i_brand STRING, i_class_id NUMBER, i_class STRING, i_category_id NUMBER, i_category STRING, i_manufact_id NUMBER, i_manufact STRING, i_size STRING, i_formulation STRING, i_color STRING, i_units STRING, i_container STRING, i_manager_id NUMBER, i_product_name STRING                                                                                                                                                                                                                                                                                                                                              |
    | store        | s_store_sk NUMBER, s_store_id STRING, s_store_name STRING, s_number_employees NUMBER, s_floor_space NUMBER, s_hours STRING, s_manager STRING, s_market_id NUMBER, s_geography_class STRING, s_market_desc STRING, s_market_manager STRING, s_division_id NUMBER, s_division_name STRING, s_company_id NUMBER, s_company_name STRING, s_street_number STRING, s_street_name STRING, s_street_type STRING, s_suite_number STRING, s_city STRING, s_county STRING, s_state STRING, s_zip STRING, s_country STRING, s_gmt_offset NUMBER, s_tax_percentage NUMBER                                                                                                                                                                                              |
    | date_dim     | d_date_sk NUMBER, d_date_id STRING, d_date DATE, d_month_seq NUMBER, d_week_seq NUMBER, d_quarter_seq NUMBER, d_year NUMBER, d_dow NUMBER, d_moy NUMBER, d_dom NUMBER, d_qoy NUMBER, d_fy_year NUMBER, d_fy_quarter_seq NUMBER, d_fy_week_seq NUMBER, d_day_name STRING, d_quarter_name STRING, d_holiday STRING, d_weekend STRING, d_following_holiday STRING, d_first_dom NUMBER, d_last_dom NUMBER, d_same_day_ly NUMBER, d_same_day_lq NUMBER, d_current_day STRING, d_current_week STRING, d_current_month STRING, d_current_quarter STRING, d_current_year STRING |

Scenario: Create fact_monthly_sales table
  Given target schema has fact table:
    | table_name        | columns                                                                                                                                                                                                   |
    | fact_monthly_sales | sales_month STRING, item_sk NUMBER, store_sk NUMBER, total_sales_amount NUMBER, total_quantity NUMBER, total_net_profit NUMBER, avg_sales_price NUMBER, total_discount_amount NUMBER |

  And target schema has dimension table:
    | table_name | columns                                                                                                                                                             |
    | dim_item   | item_sk NUMBER, item_id STRING, item_desc STRING, brand STRING, category STRING, class STRING, manufacturer STRING, product_name STRING |

  And target schema has dimension table:
    | table_name | columns                                                                                                                                                   |
    | dim_store  | store_sk NUMBER, store_id STRING, store_name STRING, city STRING, state STRING, country STRING, market_desc STRING, division_name STRING, company_name STRING |

  When I join store_sales to date_dim on ss_sold_date_sk equals d_date_sk
  And I join store_sales to item on ss_item_sk equals i_item_sk  
  And I join store_sales to store on ss_store_sk equals s_store_sk
  Then I aggregate sales data by year and month from date dimension and by item and store
  And I calculate total sales amount as sum of extended sales price
  And I calculate total quantity as sum of quantity sold
  And I calculate total net profit as sum of net profit
  And I calculate average sales price as total sales amount divided by total quantity
  And I calculate total discount amount as sum of extended discount amount
  And I create sales month as concatenation of year and month from date dimension
  And I populate fact_monthly_sales with aggregated metrics grouped by sales month, item key, and store key

Scenario: Create dim_item dimension table
  When I select distinct items from item table
  Then I extract item surrogate key as primary key
  And I extract item id for business key reference
  And I extract item description for readable name
  And I extract brand name for product categorization
  And I extract category name for product grouping
  And I extract class name for product classification
  And I extract manufacturer name for supplier information
  And I extract product name for detailed identification
  And I populate dim_item with selected item attributes

Scenario: Create dim_store dimension table  
  When I select distinct stores from store table
  Then I extract store surrogate key as primary key
  And I extract store id for business key reference  
  And I extract store name for identification
  And I extract city for location information
  And I extract state for regional information
  And I extract country for geographic information
  And I extract market description for market segmentation
  And I extract division name for organizational structure
  And I extract company name for corporate hierarchy
  And I populate dim_store with selected store attributes