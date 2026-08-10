{{ config(
    materialized='table'
) }}

with credit as (
    select * from {{ ref('stg_credit_scores') }}
),

customers as (
    select customer_id, customer_key, annual_income from {{ ref('dim_customer') }}
)

select
    c.customer_key,
    cr.customer_id,
    cr.credit_score,
    case
        when cr.credit_score >= 800 then 'Excellent'
        when cr.credit_score >= 740 then 'Very Good'
        when cr.credit_score >= 670 then 'Good'
        when cr.credit_score >= 580 then 'Fair'
        else 'Poor'
    end as credit_rating,
    cr.score_date,
    cr.credit_history_length,
    cr.number_of_accounts,
    cr.total_debt,
    cr.credit_utilization,
    (cr.total_debt / nullif(c.annual_income, 0)) as debt_to_income_ratio,
    case
        when cr.credit_utilization > 0.8 then 'High'
        when cr.credit_utilization > 0.5 then 'Medium'
        else 'Low'
    end as utilization_risk_level,
    case
        when (cr.total_debt / nullif(c.annual_income, 0)) > 0.4 then true
        else false
    end as high_debt_burden,
    current_timestamp as created_at,
    current_timestamp as updated_at
from credit cr
left join customers c on cr.customer_id = c.customer_id
