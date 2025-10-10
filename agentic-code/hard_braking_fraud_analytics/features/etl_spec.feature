# Hard Braking and Fraud Analytics Data Product
# Similar patterns referenced from SCRUM-8, SCRUM-9 aggregation approaches

Feature: hard_braking_fraud_analytics
  As a risk analyst
  I want to analyze hard braking patterns and fraudulent claims
  So that I can identify rash driving behaviors and correlate with fraud risk

  Background:
    Given the following source tables exist:
      | Table Name        | Description                           |
      | telematics        | Driving behavior and trip metrics     |
      | claims            | Insurance claims and fraud indicators |
      | policy_customers  | Customer and policy information       |

  Scenario: Create hard braking fraud analytics data model
    Given the source schema:
      """
      telematics:
        - trip_id: STRING
        - policy_id: STRING  
        - start_time: DATE
        - end_time: DATE
        - duration_minutes: NUMBER
        - miles: NUMBER
        - avg_speed_mph: NUMBER
        - hard_brakes: NUMBER
        - rapid_accels: NUMBER
        - night_driving_pct: NUMBER
        - weather: STRING
        - start_lat: NUMBER
        - start_lon: NUMBER

      claims:
        - claim_id: STRING
        - policy_id: STRING
        - claim_date: DATE
        - report_date: DATE
        - cause: STRING
        - severity: STRING
        - incurred_amount_usd: NUMBER
        - paid_amount_usd: NUMBER
        - is_open: STRING
        - suspected_fraud: STRING
        - photos_count: NUMBER
        - adjuster_notes_length_chars: NUMBER

      policy_customers:
        - policy_id: STRING
        - c_id: STRING
        - customer_name: STRING
        - email: STRING
        - phone: STRING
        - address: STRING
        - policy_type: STRING
        - policy_start_date: STRING
        - policy_end_date: STRING
        - policy_term_months: NUMBER
        - annual_premium_usd: NUMBER
        - agent_id: STRING
        - multi_policy_discount: STRING
        - safe_driver_discount: STRING
        - state: STRING
      """

    When I transform the data
    Then I should create target schema:
      """
      hard_braking_fraud_analytics:
        - policy_id: STRING
        - customer_name: STRING
        - state: STRING
        - policy_type: STRING
        - total_trips: NUMBER
        - total_miles: NUMBER
        - total_hard_brakes: NUMBER
        - hard_brakes_per_10_miles: NUMBER
        - is_rash_driver: STRING
        - avg_speed_mph: NUMBER
        - night_driving_pct: NUMBER
        - total_claims: NUMBER
        - fraudulent_claims: NUMBER
        - fraud_claim_rate: NUMBER
        - total_incurred_amount: NUMBER
        - total_paid_amount: NUMBER
        - avg_claim_amount: NUMBER
        - analysis_date: DATE
      """

    And the join relationships should be:
      """
      telematics LEFT JOIN policy_customers ON telematics.policy_id = policy_customers.policy_id
      LEFT JOIN claims ON policy_customers.policy_id = claims.policy_id
      """

    And the transformation logic should be:
      """
      Group data by policy_id and customer information
      Aggregate total trips as count of distinct trip_id from telematics
      Aggregate total miles as sum of miles from telematics
      Aggregate total hard brakes as sum of hard_brakes from telematics
      Calculate hard brakes per 10 miles as total_hard_brakes divided by total_miles multiplied by 10
      Classify as rash driver if hard_brakes_per_10_miles is greater than or equal to 1
      Calculate average speed as average of avg_speed_mph from telematics
      Calculate average night driving percentage as average of night_driving_pct from telematics
      Count total claims as count of distinct claim_id from claims
      Count fraudulent claims as count of claims where suspected_fraud is true
      Calculate fraud claim rate as fraudulent_claims divided by total_claims
      Sum total incurred amount as sum of incurred_amount_usd from claims
      Sum total paid amount as sum of paid_amount_usd from claims
      Calculate average claim amount as total_incurred_amount divided by total_claims
      Set analysis_date as current date
      """