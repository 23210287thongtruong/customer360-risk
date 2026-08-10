{{ config(
    materialized='table'
) }}

with customers as (
    select * from {{ ref('dim_customer') }}
),

credit as (
    select * from {{ ref('dim_credit') }}
),

transactions as (
    select * from {{ ref('fact_transactions') }}
),

transaction_metrics as (
    select
        customer_id,
        count(transaction_id) as total_transactions,
        coalesce(sum(amount), 0) as total_spent,
        coalesce(avg(amount), 0) as avg_transaction_amount,
        coalesce(max(amount), 0) as max_transaction_amount,
        coalesce(min(amount), 0) as min_transaction_amount,
        coalesce(stddev(amount), 0) as transaction_amount_stddev,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date,
        extract(day from current_date - max(transaction_date)) as days_since_last_transaction,
        -- Need mode/first for favorite_merchant_category, simple workaround:
        max(merchant_category) as favorite_merchant_category,
        count(distinct merchant_category) as unique_merchant_categories,
        count(distinct merchant_name) as unique_merchants,
        coalesce(sum(case when is_online then 1 else 0 end)::float / nullif(count(*), 0), 0) as online_transaction_pct,
        coalesce(sum(case when is_weekend then 1 else 0 end)::float / nullif(count(*), 0), 0) as weekend_transaction_pct,
        coalesce(sum(case when is_fraud then 1 else 0 end), 0) as fraud_count,
        coalesce(sum(case when is_fraud then amount else 0 end), 0) as fraud_amount,
        coalesce(avg(transaction_hour), 0) as avg_transaction_hour,
        coalesce(sum(case when amount > 500 then 1 else 0 end), 0) as high_value_transaction_count
    from transactions
    group by customer_id
),

customer_360_base as (
    select
        c.customer_key,
        c.customer_id,
        c.name,
        c.date_of_birth,
        c.age,
        c.city,
        c.state,
        c.annual_income,
        c.income_bracket,
        c.job_title,
        c.employment_status,
        c.marital_status,
        c.customer_tenure_days,
        
        coalesce(tm.total_transactions, 0) as total_transactions,
        coalesce(tm.total_spent, 0) as total_spent,
        coalesce(tm.avg_transaction_amount, 0) as avg_transaction_amount,
        coalesce(tm.max_transaction_amount, 0) as max_transaction_amount,
        coalesce(tm.min_transaction_amount, 0) as min_transaction_amount,
        coalesce(tm.transaction_amount_stddev, 0) as transaction_amount_stddev,
        tm.first_transaction_date,
        tm.last_transaction_date,
        coalesce(tm.days_since_last_transaction, 9999) as days_since_last_transaction,
        tm.favorite_merchant_category,
        coalesce(tm.unique_merchant_categories, 0) as unique_merchant_categories,
        coalesce(tm.unique_merchants, 0) as unique_merchants,
        coalesce(tm.online_transaction_pct, 0) as online_transaction_pct,
        coalesce(tm.weekend_transaction_pct, 0) as weekend_transaction_pct,
        coalesce(tm.fraud_count, 0) as fraud_count,
        coalesce(tm.fraud_amount, 0) as fraud_amount,
        coalesce(tm.high_value_transaction_count, 0) as high_value_transaction_count,
        
        coalesce(cr.credit_score, 300) as credit_score,
        coalesce(cr.credit_rating, 'Poor') as credit_rating,
        coalesce(cr.credit_history_length, 0) as credit_history_length,
        coalesce(cr.total_debt, 0) as total_debt,
        coalesce(cr.credit_utilization, 0) as credit_utilization,
        coalesce(cr.debt_to_income_ratio, 0) as debt_to_income_ratio
    from customers c
    left join credit cr on c.customer_key = cr.customer_key
    left join transaction_metrics tm on c.customer_id = tm.customer_id
),

risk_scored as (
    select
        *,
        -- Raw risk calculation
        (
            ((850.0 - coalesce(credit_score, 600)) / 850.0 * 40.0) +
            (least(coalesce(credit_utilization, 0.5), 1.0) * 20.0) +
            (least(coalesce(debt_to_income_ratio, 0.2) / 0.5, 1.0) * 25.0) +
            (least(coalesce(transaction_amount_stddev, 100) / 1000.0, 1.0) * 10.0) +
            (least(coalesce(days_since_last_transaction, 0) / 180.0, 1.0) * 5.0)
        ) as raw_risk_score
    from customer_360_base
)

select
    *,
    -- Bucket the risk score
    case
        when raw_risk_score < 20.0 then 'Very Low'
        when raw_risk_score < 40.0 then 'Low'
        when raw_risk_score < 60.0 then 'Medium'
        when raw_risk_score < 80.0 then 'High'
        else 'Very High'
    end as risk_category,
    
    raw_risk_score as risk_score,
    
    -- Array of risk factors (using postgres array syntax)
    array_remove(
        array[
            case when credit_score < 600 then 'Low Credit Score' else null end,
            case when credit_utilization > 0.8 then 'High Credit Utilization' else null end,
            case when debt_to_income_ratio > 0.4 then 'High Debt to Income' else null end,
            case when total_spent > 10000 then 'High Spending Volume' else null end,
            case when annual_income < 30000 then 'Low Income' else null end,
            case when days_since_last_transaction > 90 then 'Inactive Customer' else null end,
            case when fraud_count > 0 then 'Fraud History' else null end,
            case when transaction_amount_stddev > 500 then 'High Transaction Volatility' else null end
        ],
        null
    ) as risk_factors,
    
    current_timestamp as ml_scored_at,
    'SQL Risk Model v1.0' as scoring_model_version,
    current_timestamp as last_updated
from risk_scored
