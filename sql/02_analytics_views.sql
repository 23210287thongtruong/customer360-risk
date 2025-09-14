CREATE OR REPLACE VIEW analytics.v_customer_risk_summary AS
SELECT risk_category,
  COUNT(*) as customer_count,
  ROUND(
    COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100,
    2
  ) as percentage,
  AVG(risk_score) as avg_risk_score,
  AVG(credit_score) as avg_credit_score,
  AVG(annual_income) as avg_income,
  AVG(total_spent) as avg_total_spent
FROM analytics.customer_360
GROUP BY risk_category
ORDER BY CASE
    risk_category
    WHEN 'Low' THEN 1
    WHEN 'Medium' THEN 2
    WHEN 'High' THEN 3
  END;
CREATE OR REPLACE VIEW analytics.v_monthly_transaction_trends AS
SELECT DATE_TRUNC('month', transaction_date) as month,
  COUNT(*) as transaction_count,
  SUM(amount) as total_volume,
  AVG(amount) as avg_transaction_amount,
  COUNT(DISTINCT customer_id) as active_customers,
  SUM(
    CASE
      WHEN is_fraud THEN 1
      ELSE 0
    END
  ) as fraud_count
FROM warehouse.fact_transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '24 months'
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;
CREATE OR REPLACE VIEW analytics.v_high_value_customers AS
SELECT c.*,
  CASE
    WHEN total_spent > 20000
    AND credit_score > 700 THEN 'Premium'
    WHEN total_spent > 10000
    AND credit_score > 650 THEN 'Gold'
    WHEN total_spent > 5000
    AND credit_score > 600 THEN 'Silver'
    ELSE 'Bronze'
  END as customer_tier
FROM analytics.customer_360 c
WHERE risk_category IN ('Low', 'Medium')
  AND total_transactions > 10
ORDER BY total_spent DESC;
CREATE OR REPLACE VIEW analytics.v_risk_factor_analysis AS WITH risk_factors_expanded AS (
    SELECT customer_id,
      unnest(risk_factors) as risk_factor
    FROM analytics.customer_360
    WHERE array_length(risk_factors, 1) > 0
  )
SELECT risk_factor,
  COUNT(*) as customer_count,
  ROUND(
    COUNT(*)::numeric / (
      SELECT COUNT(*)
      FROM analytics.customer_360
    ) * 100,
    2
  ) as percentage_of_total
FROM risk_factors_expanded
GROUP BY risk_factor
ORDER BY customer_count DESC;
CREATE OR REPLACE VIEW analytics.v_customer_segmentation AS
SELECT income_bracket,
  risk_category,
  COUNT(*) as customer_count,
  AVG(credit_score) as avg_credit_score,
  AVG(total_spent) as avg_total_spent,
  AVG(risk_score) as avg_risk_score
FROM analytics.customer_360
GROUP BY income_bracket,
  risk_category
ORDER BY income_bracket,
  risk_category;
CREATE OR REPLACE FUNCTION analytics.calculate_customer_ltv(p_customer_id VARCHAR(20)) RETURNS TABLE(
    customer_id VARCHAR(20),
    ltv_score DECIMAL(10, 2),
    avg_monthly_spend DECIMAL(10, 2),
    predicted_lifetime_months INTEGER,
    churn_risk VARCHAR(20)
  ) AS $$ BEGIN RETURN QUERY WITH customer_metrics AS (
    SELECT c.customer_id,
      c.total_spent,
      c.total_transactions,
      c.days_since_last_transaction,
      EXTRACT(
        DAYS
        FROM (
            c.last_transaction_date - c.first_transaction_date
          )
      ) as customer_lifetime_days,
      c.risk_category
    FROM analytics.customer_360 c
    WHERE c.customer_id = p_customer_id
  )
SELECT cm.customer_id,
  (
    cm.total_spent / GREATEST(cm.customer_lifetime_days, 30) * 30
  ) * CASE
    WHEN cm.risk_category = 'Low' THEN 36
    WHEN cm.risk_category = 'Medium' THEN 24
    ELSE 12
  END as ltv_score,
  cm.total_spent / GREATEST(cm.customer_lifetime_days, 30) * 30 as avg_monthly_spend,
  CASE
    WHEN cm.risk_category = 'Low' THEN 36
    WHEN cm.risk_category = 'Medium' THEN 24
    ELSE 12
  END as predicted_lifetime_months,
  CASE
    WHEN cm.days_since_last_transaction > 90 THEN 'High'
    WHEN cm.days_since_last_transaction > 30 THEN 'Medium'
    ELSE 'Low'
  END as churn_risk
FROM customer_metrics cm;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION analytics.update_customer_risk_score(p_customer_id VARCHAR(20)) RETURNS VARCHAR(50) AS $$
DECLARE v_risk_score DECIMAL(5, 2);
v_risk_category VARCHAR(20);
v_credit_score INTEGER;
v_credit_utilization DECIMAL(5, 4);
v_debt_to_income DECIMAL(5, 4);
v_total_spent DECIMAL(12, 2);
v_annual_income DECIMAL(12, 2);
v_days_since_last_txn INTEGER;
BEGIN
SELECT credit_score,
  credit_utilization,
  debt_to_income_ratio,
  total_spent,
  annual_income,
  days_since_last_transaction INTO v_credit_score,
  v_credit_utilization,
  v_debt_to_income,
  v_total_spent,
  v_annual_income,
  v_days_since_last_txn
FROM analytics.customer_360
WHERE customer_id = p_customer_id;
v_risk_score := 0;
IF v_credit_score < 580 THEN v_risk_score := v_risk_score + 30;
ELSIF v_credit_score < 620 THEN v_risk_score := v_risk_score + 20;
ELSIF v_credit_score < 670 THEN v_risk_score := v_risk_score + 10;
END IF;
IF v_credit_utilization > 0.8 THEN v_risk_score := v_risk_score + 20;
ELSIF v_credit_utilization > 0.6 THEN v_risk_score := v_risk_score + 15;
ELSIF v_credit_utilization > 0.4 THEN v_risk_score := v_risk_score + 10;
END IF;
IF v_debt_to_income > 0.4 THEN v_risk_score := v_risk_score + 25;
ELSIF v_debt_to_income > 0.3 THEN v_risk_score := v_risk_score + 15;
ELSIF v_debt_to_income > 0.2 THEN v_risk_score := v_risk_score + 10;
END IF;
IF v_total_spent > 10000 THEN v_risk_score := v_risk_score + 10;
ELSIF v_total_spent > 5000 THEN v_risk_score := v_risk_score + 5;
END IF;
IF v_annual_income < 30000 THEN v_risk_score := v_risk_score + 10;
ELSIF v_annual_income < 50000 THEN v_risk_score := v_risk_score + 5;
END IF;
IF v_days_since_last_txn > 90 THEN v_risk_score := v_risk_score + 5;
ELSIF v_days_since_last_txn > 30 THEN v_risk_score := v_risk_score + 2;
END IF;
IF v_risk_score >= 60 THEN v_risk_category := 'High';
ELSIF v_risk_score >= 30 THEN v_risk_category := 'Medium';
ELSE v_risk_category := 'Low';
END IF;
UPDATE analytics.customer_360
SET risk_score = v_risk_score,
  risk_category = v_risk_category,
  last_updated = CURRENT_TIMESTAMP
WHERE customer_id = p_customer_id;
INSERT INTO analytics.customer_risk_history (
    customer_id,
    risk_score,
    risk_category,
    assessment_date,
    created_at
    VALUES (
        p_customer_id,
        v_risk_score,
        v_risk_category,
        CURRENT_DATE,
        CURRENT_TIMESTAMP
      );
RETURN v_risk_category;
END;
$$ LANGUAGE plpgsql;
DROP TABLE IF EXISTS analytics.pipeline_runs CASCADE;
CREATE TABLE analytics.pipeline_runs (
  run_id SERIAL PRIMARY KEY,
  run_date DATE NOT NULL,
  pipeline_name VARCHAR(100) NOT NULL,
  status VARCHAR(20) NOT NULL,
  records_processed INTEGER,
  run_duration DECIMAL(10, 2),
  error_message TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(run_date, pipeline_name)
);
DROP TABLE IF EXISTS analytics.data_quality_metrics CASCADE;
CREATE TABLE analytics.data_quality_metrics (
  metric_id SERIAL PRIMARY KEY,
  table_name VARCHAR(100) NOT NULL,
  metric_name VARCHAR(100) NOT NULL,
  metric_value DECIMAL(15, 4),
  threshold_value DECIMAL(15, 4),
  status VARCHAR(20),
  check_date DATE DEFAULT CURRENT_DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO analytics.data_quality_metrics (
    table_name,
    metric_name,
    metric_value,
    threshold_value,
    status
  )
VALUES (
    'staging.customers',
    'null_customer_id_pct',
    0.0,
    0.0,
    'PASS'
  ),
  (
    'staging.transactions',
    'negative_amount_pct',
    0.0,
    0.0,
    'PASS'
  ),
  (
    'staging.credit_scores',
    'invalid_score_pct',
    0.0,
    0.0,
    'PASS'
  ),
  (
    'analytics.customer_360',
    'completeness_pct',
    100.0,
    95.0,
    'PASS'
  );
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO postgres;