import duckdb

# Connect to persistent DuckDB database
conn = duckdb.connect('/Users/maheshdattamishra/insurance.db')

# Create the fraud_driving_correlation table
create_table_sql = """
CREATE OR REPLACE TABLE fraud_driving_correlation AS
WITH telematics_agg AS (
    -- Aggregate telematics data by policy_id
    SELECT 
        policy_id,
        COUNT(*) as total_trips,
        SUM(miles) as total_miles_driven,
        AVG(avg_speed_mph) as avg_speed_mph,
        SUM(hard_brakes) as total_hard_brakes,
        SUM(rapid_accels) as total_rapid_accels,
        CASE 
            WHEN SUM(miles) > 0 THEN SUM(hard_brakes) / SUM(miles)
            ELSE 0 
        END as hard_brakes_per_mile,
        CASE 
            WHEN SUM(miles) > 0 THEN SUM(rapid_accels) / SUM(miles)
            ELSE 0 
        END as rapid_accels_per_mile,
        AVG(night_driving_pct) as night_driving_pct
    FROM telematics
    GROUP BY policy_id
),

claims_agg AS (
    -- Aggregate claims data by policy_id
    SELECT 
        policy_id,
        COUNT(*) as total_claims,
        SUM(CASE WHEN suspected_fraud = 'true' THEN 1 ELSE 0 END) as fraud_claims,
        SUM(incurred_amount_usd) as total_claim_amount
    FROM claims
    GROUP BY policy_id
),

risky_driving_scores AS (
    -- Calculate risky driving score as weighted combination
    SELECT 
        policy_id,
        total_trips,
        total_miles_driven,
        avg_speed_mph,
        total_hard_brakes,
        total_rapid_accels,
        hard_brakes_per_mile,
        rapid_accels_per_mile,
        night_driving_pct,
        -- Risky driving score: weighted combination of metrics (0-100 scale)
        LEAST(100, 
            (hard_brakes_per_mile * 30) + 
            (rapid_accels_per_mile * 25) + 
            (CASE 
                WHEN avg_speed_mph > 75 THEN (avg_speed_mph - 75) * 2
                WHEN avg_speed_mph < 45 THEN (45 - avg_speed_mph) * 1.5
                ELSE 0 
            END) +
            (night_driving_pct * 0.3)
        ) as risky_driving_score
    FROM telematics_agg
),

fraud_rates AS (
    -- Calculate fraud rates
    SELECT 
        policy_id,
        total_claims,
        fraud_claims,
        total_claim_amount,
        CASE 
            WHEN total_claims > 0 THEN (fraud_claims * 100.0 / total_claims)
            ELSE 0 
        END as fraud_rate,
        CASE 
            WHEN total_claims > 0 THEN total_claim_amount / total_claims
            ELSE 0 
        END as avg_claim_amount
    FROM claims_agg
)

SELECT 
    pc.policy_id,
    pc.c_id as customer_id,
    pc.customer_name,
    pc.state,
    pc.policy_type,
    pc.annual_premium_usd,
    COALESCE(fr.total_claims, 0) as total_claims,
    COALESCE(fr.fraud_claims, 0) as fraud_claims,
    COALESCE(fr.fraud_rate, 0) as fraud_rate,
    COALESCE(fr.total_claim_amount, 0) as total_claim_amount,
    COALESCE(fr.avg_claim_amount, 0) as avg_claim_amount,
    COALESCE(rds.total_trips, 0) as total_trips,
    COALESCE(rds.total_miles_driven, 0) as total_miles_driven,
    COALESCE(rds.avg_speed_mph, 0) as avg_speed_mph,
    COALESCE(rds.total_hard_brakes, 0) as total_hard_brakes,
    COALESCE(rds.total_rapid_accels, 0) as total_rapid_accels,
    COALESCE(rds.hard_brakes_per_mile, 0) as hard_brakes_per_mile,
    COALESCE(rds.rapid_accels_per_mile, 0) as rapid_accels_per_mile,
    COALESCE(rds.night_driving_pct, 0) as night_driving_pct,
    COALESCE(rds.risky_driving_score, 0) as risky_driving_score,
    pc.safe_driver_discount as has_safe_driver_discount,
    CASE 
        WHEN COALESCE(rds.risky_driving_score, 0) < 30 AND COALESCE(fr.fraud_rate, 0) < 10 THEN 'Low Risk'
        WHEN COALESCE(rds.risky_driving_score, 0) > 70 OR COALESCE(fr.fraud_rate, 0) > 25 THEN 'High Risk'
        ELSE 'Medium Risk'
    END as behavior_fraud_risk_category
FROM policy_customers pc
LEFT JOIN fraud_rates fr ON pc.policy_id = fr.policy_id
LEFT JOIN risky_driving_scores rds ON pc.policy_id = rds.policy_id
"""

# Execute the ETL query
conn.execute(create_table_sql)

# Verify the results
result = conn.execute("SELECT COUNT(*) as total_records FROM fraud_driving_correlation").fetchone()
print(f"Created fraud_driving_correlation table with {result[0]} records")

# Show sample data
sample_data = conn.execute("""
SELECT 
    policy_id, 
    customer_name, 
    behavior_fraud_risk_category, 
    fraud_rate, 
    risky_driving_score,
    total_claims,
    total_trips
FROM fraud_driving_correlation 
LIMIT 5
""").fetchall()

print("Sample data:")
for row in sample_data:
    print(row)

# Close connection
conn.close()