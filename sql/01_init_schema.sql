CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO staging,
  warehouse,
  analytics,
  public;
DROP TABLE IF EXISTS staging.customers CASCADE;
CREATE TABLE staging.customers (
  customer_id UUID PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  date_of_birth DATE,
  address TEXT,
  city VARCHAR(100),
  state VARCHAR(50),
  zip_code VARCHAR(10),
  phone VARCHAR(20),
  email VARCHAR(255) UNIQUE NOT NULL,
  annual_income DECIMAL(12, 2),
  job_title VARCHAR(255),
  employment_status VARCHAR(50),
  marital_status VARCHAR(50),
  created_date DATE,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS staging.transactions CASCADE;
CREATE TABLE staging.transactions (
  transaction_id UUID PRIMARY KEY,
  customer_id UUID NOT NULL,
  transaction_type VARCHAR(50),
  amount DECIMAL(12, 2),
  timestamp TIMESTAMP,
  merchant_name VARCHAR(255),
  merchant_category VARCHAR(100),
  location_city VARCHAR(100),
  location_state VARCHAR(50),
  is_weekend BOOLEAN,
  hour INTEGER,
  is_online BOOLEAN,
  is_fraud BOOLEAN,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS staging.credit_scores CASCADE;
CREATE TABLE staging.credit_scores (
  customer_id UUID PRIMARY KEY,
  credit_score INTEGER CHECK (
    credit_score BETWEEN 300 AND 850
  ),
  score_date DATE,
  credit_history_length INTEGER,
  number_of_accounts INTEGER,
  total_debt DECIMAL(12, 2),
  credit_utilization DECIMAL(5, 4),
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS warehouse.dim_customer CASCADE;
CREATE TABLE warehouse.dim_customer (
  customer_key SERIAL PRIMARY KEY,
  customer_id VARCHAR(50) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL,
  date_of_birth DATE,
  age INTEGER,
  full_address TEXT,
  city VARCHAR(100),
  state VARCHAR(50),
  zip_code VARCHAR(10),
  phone VARCHAR(20),
  email VARCHAR(255) UNIQUE NOT NULL,
  annual_income DECIMAL(12, 2),
  income_bracket VARCHAR(50),
  job_title VARCHAR(255),
  employment_status VARCHAR(50),
  marital_status VARCHAR(50),
  customer_since DATE,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS warehouse.fact_transactions CASCADE;
CREATE TABLE warehouse.fact_transactions (
  transaction_key SERIAL PRIMARY KEY,
  transaction_id VARCHAR(50) UNIQUE NOT NULL,
  customer_key INTEGER REFERENCES warehouse.dim_customer(customer_key),
  customer_id VARCHAR(50) NOT NULL,
  transaction_type VARCHAR(50),
  amount DECIMAL(12, 2),
  transaction_date DATE,
  transaction_timestamp TIMESTAMP,
  merchant_name VARCHAR(255),
  merchant_category VARCHAR(100),
  location_city VARCHAR(100),
  location_state VARCHAR(50),
  is_weekend BOOLEAN,
  transaction_hour INTEGER,
  is_online BOOLEAN,
  is_fraud BOOLEAN,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS warehouse.dim_credit CASCADE;
CREATE TABLE warehouse.dim_credit (
  credit_key SERIAL PRIMARY KEY,
  customer_key INTEGER REFERENCES warehouse.dim_customer(customer_key),
  customer_id VARCHAR(50) NOT NULL,
  credit_score INTEGER CHECK (
    credit_score BETWEEN 300 AND 850
  ),
  credit_rating VARCHAR(20),
  score_date DATE,
  credit_history_length INTEGER,
  number_of_accounts INTEGER,
  total_debt DECIMAL(12, 2),
  credit_utilization DECIMAL(5, 4),
  debt_to_income_ratio DECIMAL(5, 4),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS analytics.customer_360 CASCADE;
CREATE TABLE analytics.customer_360 (
  customer_key INTEGER PRIMARY KEY REFERENCES warehouse.dim_customer(customer_key),
  customer_id VARCHAR(50) UNIQUE NOT NULL,
  name VARCHAR(255),
  date_of_birth DATE,
  age INTEGER,
  city VARCHAR(100),
  state VARCHAR(50),
  annual_income DECIMAL(12, 2),
  income_bracket VARCHAR(50),
  job_title VARCHAR(255),
  employment_status VARCHAR(50),
  marital_status VARCHAR(50),
  total_transactions INTEGER,
  total_spent DECIMAL(12, 2),
  avg_transaction_amount DECIMAL(12, 2),
  max_transaction_amount DECIMAL(12, 2),
  first_transaction_date DATE,
  last_transaction_date DATE,
  days_since_last_transaction INTEGER,
  favorite_merchant_category VARCHAR(100),
  online_transaction_pct DECIMAL(5, 4),
  weekend_transaction_pct DECIMAL(5, 4),
  credit_score INTEGER,
  credit_rating VARCHAR(20),
  credit_history_length INTEGER,
  total_debt DECIMAL(12, 2),
  credit_utilization DECIMAL(5, 4),
  debt_to_income_ratio DECIMAL(5, 4),
  risk_score DECIMAL(5, 2),
  risk_category VARCHAR(50),
  risk_factors TEXT [],
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS analytics.risk_scoring_rules CASCADE;
CREATE TABLE analytics.risk_scoring_rules (
  rule_id SERIAL PRIMARY KEY,
  rule_name VARCHAR(100) NOT NULL,
  rule_description TEXT,
  rule_weight DECIMAL(5, 4),
  threshold_value DECIMAL(12, 2),
  risk_points INTEGER,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS analytics.customer_risk_history CASCADE;
CREATE TABLE analytics.customer_risk_history (
  history_id SERIAL PRIMARY KEY,
  customer_id VARCHAR(50) NOT NULL,
  risk_score DECIMAL(5, 2),
  risk_category VARCHAR(50),
  risk_factors TEXT [],
  assessment_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- =====================================
-- INDEXES FOR PERFORMANCE
-- =====================================
-- Staging table indexes
CREATE INDEX idx_staging_customers_created_date ON staging.customers(created_date);
CREATE INDEX idx_staging_transactions_customer_id ON staging.transactions(customer_id);
CREATE INDEX idx_staging_transactions_timestamp ON staging.transactions(timestamp);
CREATE INDEX idx_staging_credit_scores_score_date ON staging.credit_scores(score_date);
-- Warehouse table indexes
CREATE INDEX idx_dim_customer_customer_id ON warehouse.dim_customer(customer_id);
CREATE INDEX idx_fact_transactions_customer_key ON warehouse.fact_transactions(customer_key);
CREATE INDEX idx_fact_transactions_customer_id ON warehouse.fact_transactions(customer_id);
CREATE INDEX idx_fact_transactions_date ON warehouse.fact_transactions(transaction_date);
CREATE INDEX idx_dim_credit_customer_key ON warehouse.dim_credit(customer_key);
CREATE INDEX idx_dim_credit_customer_id ON warehouse.dim_credit(customer_id);
-- Analytics table indexes
CREATE INDEX idx_customer_360_risk_category ON analytics.customer_360(risk_category);
CREATE INDEX idx_customer_360_credit_score ON analytics.customer_360(credit_score);
CREATE INDEX idx_customer_360_income_bracket ON analytics.customer_360(income_bracket);
-- =====================================
-- INSERT DEFAULT RISK SCORING RULES
-- =====================================
INSERT INTO analytics.risk_scoring_rules (
    rule_name,
    rule_description,
    rule_weight,
    threshold_value,
    risk_points
  )
VALUES (
    'Low Credit Score',
    'Credit score below 600',
    0.3,
    600,
    30
  ),
  (
    'High Credit Utilization',
    'Credit utilization above 80%',
    0.2,
    0.8,
    20
  ),
  (
    'High Debt to Income',
    'Debt to income ratio above 40%',
    0.25,
    0.4,
    25
  ),
  (
    'High Transaction Volume',
    'Monthly spending above $5000',
    0.1,
    5000,
    10
  ),
  (
    'Low Income',
    'Annual income below $30,000',
    0.1,
    30000,
    10
  ),
  (
    'Fraud History',
    'Previous fraudulent transactions',
    0.05,
    1,
    5
  );
-- =====================================
-- UTILITY FUNCTIONS
-- =====================================
-- Function to calculate age from date of birth
CREATE OR REPLACE FUNCTION calculate_age(birth_date DATE) RETURNS INTEGER AS $$ BEGIN RETURN EXTRACT(
    YEAR
    FROM AGE(CURRENT_DATE, birth_date)
  );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- Function to determine income bracket
CREATE OR REPLACE FUNCTION get_income_bracket(income DECIMAL) RETURNS VARCHAR(50) AS $$ BEGIN CASE
    WHEN income < 30000 THEN RETURN 'Low Income';
WHEN income < 50000 THEN RETURN 'Lower Middle';
WHEN income < 75000 THEN RETURN 'Middle';
WHEN income < 100000 THEN RETURN 'Upper Middle';
ELSE RETURN 'High Income';
END CASE
;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- Function to determine credit rating
CREATE OR REPLACE FUNCTION get_credit_rating(score INTEGER) RETURNS VARCHAR(20) AS $$ BEGIN CASE
    WHEN score >= 800 THEN RETURN 'Excellent';
WHEN score >= 740 THEN RETURN 'Very Good';
WHEN score >= 670 THEN RETURN 'Good';
WHEN score >= 580 THEN RETURN 'Fair';
ELSE RETURN 'Poor';
END CASE
;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA warehouse TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA warehouse TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics TO postgres;
-- Display table information
SELECT schemaname,
  COUNT(*) as table_count
FROM pg_tables
WHERE schemaname IN ('staging', 'warehouse', 'analytics')
GROUP BY schemaname
ORDER BY schemaname;