# Data Product: driving_behavior_fraud_correlation
# This specification demonstrates correlation analysis between behavioral patterns and fraud indicators
# Adapted from retail return patterns as a proxy for fraud detection methodology
# No similar fraud detection tickets found in knowledge base - creating new pattern

Feature: Driving Behavior Fraud Correlation Data Product

  Background: Source and Target Schema Definition
    Given the source tables with schema:
      | table_name       | column_name           | data_type | description                    |
      | customer         | c_customer_sk         | NUMBER    | customer surrogate key         |
      | customer         | c_customer_id         | STRING    | customer identifier           |
      | customer         | c_birth_year          | NUMBER    | customer birth year           |
      | customer         | c_preferred_cust_flag | STRING    | preferred customer indicator  |
      | store_returns    | sr_customer_sk        | NUMBER    | customer surrogate key        |
      | store_returns    | sr_returned_date_sk   | NUMBER    | return date key               |
      | store_returns    | sr_return_quantity    | NUMBER    | quantity returned             |
      | store_returns    | sr_return_amt         | NUMBER    | return amount                 |
      | store_returns    | sr_net_loss           | NUMBER    | net loss from return          |
      | store_returns    | sr_reason_sk          | NUMBER    | return reason key             |
      | reason           | r_reason_sk           | NUMBER    | reason surrogate key          |
      | reason           | r_reason_desc         | STRING    | reason description            |
      | date_dim         | d_date_sk             | NUMBER    | date surrogate key            |
      | date_dim         | d_date                | DATE      | calendar date                 |
      | date_dim         | d_year                | NUMBER    | year                          |
      | date_dim         | d_month_seq           | NUMBER    | month sequence                |

    And the target table schema:
      | table_name                     | column_name              | data_type | description                           |
      | driving_behavior_fraud_correlation | customer_id           | STRING    | unique customer identifier            |
      | driving_behavior_fraud_correlation | customer_age          | NUMBER    | customer age                          |
      | driving_behavior_fraud_correlation | preferred_customer    | STRING    | preferred customer status             |
      | driving_behavior_fraud_correlation | total_return_incidents | NUMBER   | total number of return incidents      |
      | driving_behavior_fraud_correlation | total_return_amount   | NUMBER    | total monetary value of returns       |
      | driving_behavior_fraud_correlation | avg_return_frequency  | NUMBER    | average returns per month             |
      | driving_behavior_fraud_correlation | suspicious_reasons_count | NUMBER | count of suspicious return reasons    |
      | driving_behavior_fraud_correlation | net_loss_total        | NUMBER    | total net loss from returns           |
      | driving_behavior_fraud_correlation | fraud_risk_score      | NUMBER    | calculated fraud risk score           |
      | driving_behavior_fraud_correlation | risk_category         | STRING    | risk categorization                   |
      | driving_behavior_fraud_correlation | first_return_date     | DATE      | date of first return                  |
      | driving_behavior_fraud_correlation | last_return_date      | DATE      | date of most recent return            |
      | driving_behavior_fraud_correlation | analysis_date         | DATE      | date of analysis                      |

  Scenario: Extract and Transform Customer Behavior Data
    Given I need to join customer data with return behavior patterns
    When I extract data from source tables
    Then I should join customer table with store_returns table on customer surrogate key
    And I should join store_returns table with reason table on reason surrogate key
    And I should join store_returns table with date_dim table on returned date surrogate key

  Scenario: Calculate Behavioral Risk Metrics
    Given I have joined customer and return data
    When I calculate behavioral metrics per customer
    Then I should aggregate total return incidents by counting distinct return transactions per customer
    And I should sum total return amounts per customer
    And I should calculate average return frequency by dividing total incidents by months active
    And I should count suspicious return reasons where reason description contains words indicating potential fraud
    And I should sum net loss amounts per customer

  Scenario: Generate Fraud Risk Score
    Given I have calculated behavioral metrics
    When I compute fraud risk scores
    Then I should create weighted fraud risk score using formula combining return frequency, suspicious reasons count, and net loss amount
    And I should normalize risk score to scale of zero to one hundred
    And I should categorize risk as LOW for scores below thirty, MEDIUM for scores thirty to seventy, HIGH for scores above seventy

  Scenario: Populate Target Data Product
    Given I have calculated all metrics and risk scores
    When I populate the driving behavior fraud correlation table
    Then I should insert customer identifier from customer table
    And I should calculate customer age by subtracting birth year from current year
    And I should map preferred customer flag to preferred customer status
    And I should insert calculated behavioral metrics including return incidents, amounts, and frequencies
    And I should insert calculated fraud risk score and risk category
    And I should insert minimum and maximum return dates as first and last return dates
    And I should insert current date as analysis date
    And I should ensure all records have valid customer identifiers and non-null risk scores