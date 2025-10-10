# Data Product: driving_behavior_fraud_correlation
# Reference: Adapted pattern from SCRUM-8 (Sales Summary) and SCRUM-9 (Monthly Analytics) for aggregation and correlation analysis
# Note: This specification assumes future availability of driving behavior and claims data tables

Feature: Driving Behavior and Fraudulent Claims Correlation Analysis
  As a fraud analyst
  I want to analyze correlation between driving behaviors and fraudulent claims
  So that I can identify patterns and improve fraud detection

  Background:
    Given the following source tables exist:
      | table_name           | description                           |
      | driving_behaviors    | Contains telematics and driving data  |
      | insurance_claims     | Contains all insurance claims data    |
      | policy_holders       | Contains policy holder information    |
      | claim_investigations | Contains fraud investigation results  |

  Scenario: Build driving behavior fraud correlation data model
    Given the source schema:
      """
      driving_behaviors:
        - policy_id: STRING
        - driver_id: STRING  
        - behavior_date: DATE
        - harsh_braking_count: NUMBER
        - rapid_acceleration_count: NUMBER
        - speeding_incidents: NUMBER
        - night_driving_hours: NUMBER
        - weekend_driving_hours: NUMBER
        - total_miles_driven: NUMBER
        - average_speed: NUMBER
        - phone_usage_while_driving: NUMBER

      insurance_claims:
        - claim_id: STRING
        - policy_id: STRING
        - claim_date: DATE
        - claim_amount: NUMBER
        - claim_type: STRING
        - incident_location: STRING
        - incident_description: STRING
        - claim_status: STRING

      policy_holders:
        - policy_id: STRING
        - customer_id: STRING
        - policy_start_date: DATE
        - policy_end_date: DATE
        - vehicle_year: NUMBER
        - vehicle_make: STRING
        - vehicle_model: STRING
        - driver_age: NUMBER
        - driver_experience_years: NUMBER

      claim_investigations:
        - claim_id: STRING
        - investigation_date: DATE
        - fraud_indicator: STRING
        - fraud_confidence_score: NUMBER
        - investigation_notes: STRING
      """

    When I create the target schema:
      """
      driving_behavior_fraud_correlation:
        - policy_id: STRING
        - driver_id: STRING
        - analysis_month: DATE
        - total_claims: NUMBER
        - fraudulent_claims: NUMBER
        - fraud_rate: NUMBER
        - avg_harsh_braking: NUMBER
        - avg_rapid_acceleration: NUMBER
        - avg_speeding_incidents: NUMBER
        - total_night_driving: NUMBER
        - total_weekend_driving: NUMBER
        - total_miles: NUMBER
        - avg_phone_usage: NUMBER
        - risk_score: NUMBER
        - driver_age: NUMBER
        - vehicle_age: NUMBER
        - correlation_indicators: STRING

      fraud_behavior_patterns:
        - pattern_id: STRING
        - behavior_type: STRING
        - fraud_correlation: NUMBER
        - sample_size: NUMBER
        - confidence_level: NUMBER
        - pattern_description: STRING
      """

    And I define the join relationships:
      """
      driving_behaviors LEFT JOIN policy_holders ON driving_behaviors.policy_id = policy_holders.policy_id
      LEFT JOIN insurance_claims ON policy_holders.policy_id = insurance_claims.policy_id  
      LEFT JOIN claim_investigations ON insurance_claims.claim_id = claim_investigations.claim_id
      """

    Then I apply the following transformation logic:
      """
      For driving_behavior_fraud_correlation table:
      - Group data by policy_id, driver_id, and month from behavior_date
      - Calculate monthly aggregates for all driving behavior metrics
      - Count total claims and fraudulent claims per policy per month
      - Calculate fraud rate as fraudulent_claims divided by total_claims
      - Compute risk score using weighted average of normalized behavior metrics
      - Include driver demographics and vehicle information
      - Generate correlation indicators based on behavior patterns that correlate with fraud

      For fraud_behavior_patterns table:
      - Analyze correlation between each driving behavior metric and fraud indicators
      - Calculate Pearson correlation coefficients for each behavior-fraud pair  
      - Identify statistically significant patterns with confidence levels
      - Create pattern descriptions for behaviors most correlated with fraud
      - Store sample sizes for statistical validity assessment
      """

    And I implement data quality rules:
      """
      - Remove records with null policy_id or driver_id
      - Exclude driving behavior records with negative values
      - Filter out claims older than policy end date
      - Validate fraud indicators are within expected range zero to one
      - Ensure correlation calculations have minimum sample size of thirty records
      """