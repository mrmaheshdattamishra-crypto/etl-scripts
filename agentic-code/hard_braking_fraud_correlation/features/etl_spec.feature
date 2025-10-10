# Data Product: hard_braking_fraud_correlation
# Purpose: Build correlation between hard braking behaviors and fraudulent claims
# Note: Leveraging existing fraud_driving_correlation table structure as reference pattern

Feature: Hard Braking Fraud Correlation Data Product

  Background: Source and Target Schema Definition
    Given the following source tables:
      | table_name       | columns                                                                                                    |
      | telematics       | trip_id:STRING, policy_id:STRING, start_time:DATE, end_time:DATE, duration_minutes:NUMBER, miles:NUMBER, avg_speed_mph:NUMBER, hard_brakes:NUMBER, rapid_accels:NUMBER, night_driving_pct:NUMBER, weather:STRING, start_lat:NUMBER, start_lon:NUMBER |
      | claims           | claim_id:STRING, policy_id:STRING, claim_date:DATE, report_date:DATE, cause:STRING, severity:STRING, incurred_amount_usd:NUMBER, paid_amount_usd:NUMBER, is_open:STRING, suspected_fraud:STRING, photos_count:NUMBER, adjuster_notes_length_chars:NUMBER |
      | policy_customers | policy_id:STRING, c_id:STRING, customer_name:STRING, email:STRING, phone:STRING, address:STRING, policy_type:STRING, policy_start_date:STRING, policy_end_date:STRING, policy_term_months:NUMBER, annual_premium_usd:NUMBER, agent_id:STRING, multi_policy_discount:STRING, safe_driver_discount:STRING, state:STRING |

    And the target table schema:
      | table_name                  | columns                                                                                                                                                                                    |
      | hard_braking_fraud_correlation | policy_id:STRING, customer_id:STRING, customer_name:STRING, state:STRING, policy_type:STRING, annual_premium_usd:NUMBER, total_claims:NUMBER, fraud_claims:NUMBER, fraud_rate:NUMBER, total_hard_braking_events:NUMBER, hard_braking_frequency:NUMBER, avg_hard_brakes_per_trip:NUMBER, hard_braking_fraud_correlation_score:NUMBER, risk_category:STRING, total_trips:NUMBER, total_miles:NUMBER, hard_braking_intensity_level:STRING |

  Scenario: Join telematics data with policy and claims information
    Given telematics table contains driving behavior data
    And claims table contains fraud indicators
    And policy_customers table contains customer demographics
    When joining tables on policy_id
    Then create comprehensive dataset linking hard braking patterns to fraud incidents

  Scenario: Aggregate hard braking metrics by policy
    Given telematics data for each trip
    When grouping by policy_id
    Then calculate total hard braking events as sum of hard_brakes across all trips per policy
    And calculate hard braking frequency as total hard braking events divided by total trips
    And calculate average hard brakes per trip as total hard braking events divided by count of trips
    And calculate total trips as count of distinct trip_id per policy
    And calculate total miles as sum of miles across all trips per policy

  Scenario: Identify fraudulent claims patterns
    Given claims data with suspected_fraud indicators
    When grouping by policy_id
    Then calculate total claims as count of claim_id per policy
    And calculate fraud claims as count of claims where suspected_fraud equals true
    And calculate fraud rate as fraud claims divided by total claims where total claims is greater than zero

  Scenario: Calculate hard braking fraud correlation score
    Given aggregated hard braking metrics and fraud rates
    When analyzing correlation between hard braking frequency and fraud rate
    Then calculate hard braking fraud correlation score using statistical correlation between hard_braking_frequency and fraud_rate
    And assign correlation score as normalized value between zero and one hundred

  Scenario: Categorize hard braking intensity levels
    Given hard braking frequency metrics
    When evaluating hard braking patterns
    Then classify hard_braking_intensity_level as low when hard_braking_frequency is less than one per trip
    And classify hard_braking_intensity_level as medium when hard_braking_frequency is between one and three per trip
    And classify hard_braking_intensity_level as high when hard_braking_frequency is greater than three per trip

  Scenario: Assign risk categories based on correlation analysis
    Given hard braking fraud correlation scores
    When categorizing risk levels
    Then assign risk_category as low_risk when hard_braking_fraud_correlation_score is less than thirty
    And assign risk_category as medium_risk when hard_braking_fraud_correlation_score is between thirty and seventy
    And assign risk_category as high_risk when hard_braking_fraud_correlation_score is greater than seventy

  Scenario: Enrich with customer demographic information
    Given policy_customers table with customer details
    When joining on policy_id
    Then include customer_id as c_id from policy_customers
    And include customer_name from policy_customers
    And include state from policy_customers
    And include policy_type from policy_customers
    And include annual_premium_usd from policy_customers