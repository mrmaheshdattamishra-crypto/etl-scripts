# ETL Specification for Customer Country Dimension Enhancement
# Referenced similar ticket: SCRUM-9 (CREATE A MONTHLY SALE ANALYTIC DATA PRODUCT)
# Building upon existing monthly sales analytics pattern to add country dimension

Feature: customer_country_monthly_sales_analytics
  As a business analyst
  I want to analyze monthly sales by customer country
  So that I can understand geographic sales performance patterns

  Background: Source and Target Schema
    Given the following source tables exist:
      | Table Name | Schema |
      | policy_customers | policy_id:STRING, c_id:STRING, customer_name:STRING, email:STRING, phone:STRING, address:STRING, policy_type:STRING, policy_start_date:STRING, policy_end_date:STRING, policy_term_months:NUMBER, annual_premium_usd:NUMBER, agent_id:STRING, multi_policy_discount:STRING, safe_driver_discount:STRING, state:STRING |
      | financial_transactions | transaction_id:STRING, policy_id:STRING, date:DATE, type:STRING, amount_usd:NUMBER, payment_method:STRING, reconciled:STRING |
      | property_geospatial | property_id:STRING, address:STRING, property_type:STRING, year_built:NUMBER, square_feet:NUMBER, roof_type:STRING, construction_type:STRING, flood_zone:STRING, wildfire_risk_score:NUMBER, hail_risk_score:NUMBER, lat:NUMBER, lon:NUMBER |
    
    And the following target tables will be created:
      | Table Name | Schema |
      | dim_customer_country | customer_country_key:NUMBER, c_id:STRING, customer_name:STRING, country:STRING, state:STRING, address:STRING, created_date:DATE, updated_date:DATE |
      | fact_monthly_sales_by_country | sales_country_key:NUMBER, customer_country_key:NUMBER, year_month:STRING, sales_amount:NUMBER, transaction_count:NUMBER, unique_customers:NUMBER, created_date:DATE |

  Scenario: Create customer country dimension table
    Given source data from policy_customers table
    When transforming customer data
    Then extract country from customer address using address parsing
    And derive country as United States for all US state codes
    And create unique customer country records with surrogate key
    And populate dim_customer_country with customer country information
    And set created_date and updated_date to current timestamp

  Scenario: Build monthly sales fact table by country
    Given source data from financial_transactions and policy_customers tables
    And existing dim_customer_country dimension
    When joining financial_transactions to policy_customers on policy_id
    And joining to dim_customer_country on c_id
    And filtering for transaction type equals premium payment or fee payment
    And grouping by customer country and year-month from transaction date
    Then calculate total sales amount per country per month
    And count total number of transactions per country per month  
    And count unique customers per country per month
    And create surrogate key for each country-month combination
    And populate fact_monthly_sales_by_country table
    And set created_date to current timestamp

  Scenario: Handle data quality and transformations
    Given source customer address data may contain inconsistent formats
    When processing customer addresses
    Then standardize address formats before country extraction
    And handle null or empty address values by marking country as Unknown
    And validate state codes against standard US state abbreviations
    And log any address parsing failures for manual review
    
    Given transaction dates may span multiple years
    When creating year-month groupings
    Then format year-month as YYYY-MM for consistent sorting
    And handle null transaction dates by excluding from monthly aggregation
    And ensure date filters capture complete months only