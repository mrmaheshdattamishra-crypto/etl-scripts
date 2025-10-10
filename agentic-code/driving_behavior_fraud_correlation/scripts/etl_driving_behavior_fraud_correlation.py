import duckdb

# Create connection to persistent database
conn = duckdb.connect('./data/my_tpc.db')

# Create source tables with sample data as specified in the gherkin
conn.execute("""
CREATE TABLE IF NOT EXISTS driving_behaviors (
    policy_id VARCHAR,
    driver_id VARCHAR,
    behavior_date DATE,
    harsh_braking_count INTEGER,
    rapid_acceleration_count INTEGER,
    speeding_incidents INTEGER,
    night_driving_hours DOUBLE,
    weekend_driving_hours DOUBLE,
    total_miles_driven DOUBLE,
    average_speed DOUBLE,
    phone_usage_while_driving DOUBLE
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS insurance_claims (
    claim_id VARCHAR,
    policy_id VARCHAR,
    claim_date DATE,
    claim_amount DOUBLE,
    claim_type VARCHAR,
    incident_location VARCHAR,
    incident_description VARCHAR,
    claim_status VARCHAR
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS policy_holders (
    policy_id VARCHAR,
    customer_id VARCHAR,
    policy_start_date DATE,
    policy_end_date DATE,
    vehicle_year INTEGER,
    vehicle_make VARCHAR,
    vehicle_model VARCHAR,
    driver_age INTEGER,
    driver_experience_years INTEGER
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS claim_investigations (
    claim_id VARCHAR,
    investigation_date DATE,
    fraud_indicator VARCHAR,
    fraud_confidence_score DOUBLE,
    investigation_notes VARCHAR
)
""")

# Insert sample data for demonstration
conn.execute("""
INSERT INTO driving_behaviors VALUES
('POL001', 'DRV001', '2024-01-15', 5, 3, 2, 2.5, 4.0, 250.0, 65.5, 0.2),
('POL001', 'DRV001', '2024-02-15', 8, 5, 4, 3.0, 6.0, 300.0, 70.0, 0.4),
('POL002', 'DRV002', '2024-01-15', 2, 1, 1, 1.0, 2.0, 200.0, 55.0, 0.1),
('POL002', 'DRV002', '2024-02-15', 3, 2, 1, 1.5, 3.0, 220.0, 58.0, 0.1)
""")

conn.execute("""
INSERT INTO policy_holders VALUES
('POL001', 'CUST001', '2023-01-01', '2024-12-31', 2020, 'Toyota', 'Camry', 35, 15),
('POL002', 'CUST002', '2023-01-01', '2024-12-31', 2018, 'Honda', 'Civic', 28, 8)
""")

conn.execute("""
INSERT INTO insurance_claims VALUES
('CLM001', 'POL001', '2024-01-20', 5000.0, 'Collision', 'Highway 101', 'Rear-end collision', 'Closed'),
('CLM002', 'POL001', '2024-02-25', 12000.0, 'Theft', 'Downtown parking', 'Vehicle stolen', 'Investigating'),
('CLM003', 'POL002', '2024-02-10', 2500.0, 'Vandalism', 'Home driveway', 'Scratched paint', 'Closed')
""")

conn.execute("""
INSERT INTO claim_investigations VALUES
('CLM001', '2024-01-22', 'Low', 0.2, 'Standard collision claim'),
('CLM002', '2024-02-26', 'High', 0.8, 'Suspicious circumstances around theft'),
('CLM003', '2024-02-12', 'Low', 0.1, 'Clear vandalism case')
""")

# Create driving_behavior_fraud_correlation table
conn.execute("""
CREATE OR REPLACE TABLE driving_behavior_fraud_correlation AS
WITH monthly_data AS (
    SELECT 
        db.policy_id,
        db.driver_id,
        DATE_TRUNC('month', db.behavior_date) as analysis_month,
        AVG(db.harsh_braking_count) as avg_harsh_braking,
        AVG(db.rapid_acceleration_count) as avg_rapid_acceleration,
        AVG(db.speeding_incidents) as avg_speeding_incidents,
        SUM(db.night_driving_hours) as total_night_driving,
        SUM(db.weekend_driving_hours) as total_weekend_driving,
        SUM(db.total_miles_driven) as total_miles,
        AVG(db.phone_usage_while_driving) as avg_phone_usage,
        ph.driver_age,
        (2024 - ph.vehicle_year) as vehicle_age
    FROM driving_behaviors db
    LEFT JOIN policy_holders ph ON db.policy_id = ph.policy_id
    WHERE db.policy_id IS NOT NULL 
      AND db.driver_id IS NOT NULL
      AND db.harsh_braking_count >= 0
      AND db.rapid_acceleration_count >= 0
      AND db.speeding_incidents >= 0
    GROUP BY db.policy_id, db.driver_id, DATE_TRUNC('month', db.behavior_date), 
             ph.driver_age, ph.vehicle_year
),
claims_data AS (
    SELECT 
        ic.policy_id,
        DATE_TRUNC('month', ic.claim_date) as claim_month,
        COUNT(*) as total_claims,
        COUNT(CASE WHEN ci.fraud_confidence_score > 0.5 THEN 1 END) as fraudulent_claims
    FROM insurance_claims ic
    LEFT JOIN claim_investigations ci ON ic.claim_id = ci.claim_id
    LEFT JOIN policy_holders ph ON ic.policy_id = ph.policy_id
    WHERE ic.claim_date <= ph.policy_end_date OR ph.policy_end_date IS NULL
    GROUP BY ic.policy_id, DATE_TRUNC('month', ic.claim_date)
)
SELECT 
    md.policy_id,
    md.driver_id,
    md.analysis_month,
    COALESCE(cd.total_claims, 0) as total_claims,
    COALESCE(cd.fraudulent_claims, 0) as fraudulent_claims,
    CASE 
        WHEN COALESCE(cd.total_claims, 0) = 0 THEN 0
        ELSE CAST(COALESCE(cd.fraudulent_claims, 0) AS DOUBLE) / CAST(cd.total_claims AS DOUBLE)
    END as fraud_rate,
    md.avg_harsh_braking,
    md.avg_rapid_acceleration,
    md.avg_speeding_incidents,
    md.total_night_driving,
    md.total_weekend_driving,
    md.total_miles,
    md.avg_phone_usage,
    (md.avg_harsh_braking * 0.3 + md.avg_rapid_acceleration * 0.2 + 
     md.avg_speeding_incidents * 0.3 + md.avg_phone_usage * 0.2) as risk_score,
    md.driver_age,
    md.vehicle_age,
    CASE 
        WHEN md.avg_harsh_braking > 5 AND COALESCE(cd.fraudulent_claims, 0) > 0 THEN 'High braking with fraud'
        WHEN md.avg_speeding_incidents > 3 AND COALESCE(cd.fraudulent_claims, 0) > 0 THEN 'Speeding with fraud'
        WHEN md.avg_phone_usage > 0.3 AND COALESCE(cd.fraudulent_claims, 0) > 0 THEN 'Phone usage with fraud'
        ELSE 'No significant correlation'
    END as correlation_indicators
FROM monthly_data md
LEFT JOIN claims_data cd ON md.policy_id = cd.policy_id AND md.analysis_month = cd.claim_month
""")

# Create fraud_behavior_patterns table
conn.execute("""
CREATE OR REPLACE TABLE fraud_behavior_patterns AS
WITH correlation_analysis AS (
    SELECT 
        'harsh_braking' as behavior_type,
        CORR(avg_harsh_braking, fraud_rate) as fraud_correlation,
        COUNT(*) as sample_size
    FROM driving_behavior_fraud_correlation
    WHERE fraud_rate IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'rapid_acceleration' as behavior_type,
        CORR(avg_rapid_acceleration, fraud_rate) as fraud_correlation,
        COUNT(*) as sample_size
    FROM driving_behavior_fraud_correlation
    WHERE fraud_rate IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'speeding_incidents' as behavior_type,
        CORR(avg_speeding_incidents, fraud_rate) as fraud_correlation,
        COUNT(*) as sample_size
    FROM driving_behavior_fraud_correlation
    WHERE fraud_rate IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'phone_usage' as behavior_type,
        CORR(avg_phone_usage, fraud_rate) as fraud_correlation,
        COUNT(*) as sample_size
    FROM driving_behavior_fraud_correlation
    WHERE fraud_rate IS NOT NULL
)
SELECT 
    'PATTERN_' || UPPER(behavior_type) as pattern_id,
    behavior_type,
    COALESCE(fraud_correlation, 0.0) as fraud_correlation,
    sample_size,
    CASE 
        WHEN sample_size >= 30 THEN 0.95
        WHEN sample_size >= 20 THEN 0.90
        ELSE 0.80
    END as confidence_level,
    CASE 
        WHEN ABS(COALESCE(fraud_correlation, 0.0)) > 0.7 THEN 'Strong correlation with fraud'
        WHEN ABS(COALESCE(fraud_correlation, 0.0)) > 0.5 THEN 'Moderate correlation with fraud'
        WHEN ABS(COALESCE(fraud_correlation, 0.0)) > 0.3 THEN 'Weak correlation with fraud'
        ELSE 'No significant correlation with fraud'
    END as pattern_description
FROM correlation_analysis
WHERE sample_size >= 30
""")

# Verify the results
print("Driving Behavior Fraud Correlation Analysis:")
result1 = conn.execute("SELECT * FROM driving_behavior_fraud_correlation").fetchall()
for row in result1:
    print(row)

print("\nFraud Behavior Patterns:")
result2 = conn.execute("SELECT * FROM fraud_behavior_patterns").fetchall()
for row in result2:
    print(row)

# Close the connection
conn.close()