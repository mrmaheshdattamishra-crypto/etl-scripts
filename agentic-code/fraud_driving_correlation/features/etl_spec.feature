# No similar tickets found specifically for fraud detection and driving behavior correlation
# This creates a new pattern for insurance fraud analytics using telematics data

Feature: fraud_driving_correlation
  Background:
    Given source tables with schema:
      | table_name       | column_name                  | data_type |
      | claims           | claim_id                     | STRING    |
      | claims           | policy_id                    | STRING    |
      | claims           | claim_date                   | DATE      |
      | claims           | report_date                  | DATE      |
      | claims           | cause                        | STRING    |
      | claims           | severity                     | STRING    |
      | claims           | incurred_amount_usd          | NUMBER    |
      | claims           | paid_amount_usd              | NUMBER    |
      | claims           | is_open                      | STRING    |
      | claims           | suspected_fraud              | STRING    |
      | claims           | photos_count                 | NUMBER    |
      | claims           | adjuster_notes_length_chars  | NUMBER    |
      | telematics       | trip_id                      | STRING    |
      | telematics       | policy_id                    | STRING    |
      | telematics       | start_time                   | DATE      |
      | telematics       | end_time                     | DATE      |
      | telematics       | duration_minutes             | NUMBER    |
      | telematics       | miles                        | NUMBER    |
      | telematics       | avg_speed_mph                | NUMBER    |
      | telematics       | hard_brakes                  | NUMBER    |
      | telematics       | rapid_accels                 | NUMBER    |
      | telematics       | night_driving_pct            | NUMBER    |
      | telematics       | weather                      | STRING    |
      | telematics       | start_lat                    | NUMBER    |
      | telematics       | start_lon                    | NUMBER    |
      | policy_customers | policy_id                    | STRING    |
      | policy_customers | c_id                         | STRING    |
      | policy_customers | customer_name                | STRING    |
      | policy_customers | state                        | STRING    |
      | policy_customers | policy_type                  | STRING    |
      | policy_customers | annual_premium_usd           | NUMBER    |
      | policy_customers | safe_driver_discount         | STRING    |

    And target table with schema:
      | table_name                    | column_name                    | data_type |
      | fraud_driving_correlation     | policy_id                      | STRING    |
      | fraud_driving_correlation     | customer_id                    | STRING    |
      | fraud_driving_correlation     | customer_name                  | STRING    |
      | fraud_driving_correlation     | state                          | STRING    |
      | fraud_driving_correlation     | policy_type                    | STRING    |
      | fraud_driving_correlation     | annual_premium_usd             | NUMBER    |
      | fraud_driving_correlation     | total_claims                   | NUMBER    |
      | fraud_driving_correlation     | fraud_claims                   | NUMBER    |
      | fraud_driving_correlation     | fraud_rate                     | NUMBER    |
      | fraud_driving_correlation     | total_claim_amount             | NUMBER    |
      | fraud_driving_correlation     | avg_claim_amount               | NUMBER    |
      | fraud_driving_correlation     | total_trips                    | NUMBER    |
      | fraud_driving_correlation     | total_miles_driven             | NUMBER    |
      | fraud_driving_correlation     | avg_speed_mph                  | NUMBER    |
      | fraud_driving_correlation     | total_hard_brakes              | NUMBER    |
      | fraud_driving_correlation     | total_rapid_accels             | NUMBER    |
      | fraud_driving_correlation     | hard_brakes_per_mile           | NUMBER    |
      | fraud_driving_correlation     | rapid_accels_per_mile          | NUMBER    |
      | fraud_driving_correlation     | night_driving_pct              | NUMBER    |
      | fraud_driving_correlation     | risky_driving_score            | NUMBER    |
      | fraud_driving_correlation     | has_safe_driver_discount       | STRING    |
      | fraud_driving_correlation     | behavior_fraud_risk_category   | STRING    |

  Scenario: Build fraud and driving behavior correlation data model
    Given I have claims data with fraud indicators
    And I have telematics data with driving behavior metrics
    And I have policy customer information
    When I join tables using policy_id as the common key
    Then I create aggregated metrics for each policy holder

  Scenario: Join telematics with claims data
    Given telematics table contains driving behavior data
    And claims table contains fraud indicators
    And policy_customers table contains customer demographics
    When I join telematics to claims on policy_id
    And I join policy_customers on policy_id
    Then I have combined dataset with driving behaviors and fraud outcomes

  Scenario: Calculate driving behavior metrics
    Given combined telematics and claims data
    When I aggregate telematics data by policy_id
    Then I calculate total trips per policy
    And I calculate total miles driven per policy
    And I calculate average speed per policy
    And I calculate total hard brakes per policy
    And I calculate total rapid accelerations per policy
    And I calculate hard brakes per mile driven
    And I calculate rapid accelerations per mile driven
    And I calculate percentage of night driving per policy
    And I calculate risky driving score as weighted combination of hard brakes per mile, rapid accelerations per mile, average speed deviation, and night driving percentage

  Scenario: Calculate fraud metrics
    Given claims data with suspected fraud indicators
    When I aggregate claims by policy_id
    Then I calculate total number of claims per policy
    And I calculate number of fraud claims per policy where suspected_fraud is true
    And I calculate fraud rate as fraud claims divided by total claims
    And I calculate total claim amount per policy
    And I calculate average claim amount per policy

  Scenario: Create risk categorization
    Given calculated driving behavior metrics and fraud rates
    When I analyze the correlation between risky driving behaviors and fraud rates
    Then I categorize each policy into behavior fraud risk categories as low risk for policies with risky driving score below 30 and fraud rate below 10 percent, medium risk for policies with risky driving score between 30 and 70 or fraud rate between 10 and 25 percent, and high risk for policies with risky driving score above 70 or fraud rate above 25 percent

  Scenario: Generate final correlation dataset
    Given all calculated metrics and risk categories
    When I combine customer demographics with driving behaviors and fraud metrics
    Then I create the final fraud_driving_correlation table with all policy holders
    And I include customers with no claims as zero fraud rate
    And I include customers with no telematics data as null driving behavior metrics