{{ config(
    materialized='table'
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select customer_id, customer_key from {{ ref('dim_customer') }}
),

enriched as (
    select
        t.*,
        date(t.transaction_timestamp) as transaction_date,
        row_number() over (partition by t.customer_id order by t.transaction_timestamp) as transaction_sequence,
        sum(t.amount) over (partition by t.customer_id order by t.transaction_timestamp rows between unbounded preceding and current row) as running_total,
        extract(day from t.transaction_timestamp - lag(t.transaction_timestamp) over (partition by t.customer_id order by t.transaction_timestamp)) as days_since_last_transaction,
        t.amount - avg(t.amount) over (partition by t.customer_id) as amount_vs_avg,
        case when t.amount > 500 then true else false end as is_high_value
    from transactions t
)

select
    e.transaction_id,
    c.customer_key,
    e.customer_id,
    e.transaction_type,
    e.amount,
    e.transaction_date,
    e.transaction_timestamp,
    e.merchant_name,
    e.merchant_category,
    e.location_city,
    e.location_state,
    e.is_weekend,
    e.transaction_hour,
    e.is_online,
    e.is_fraud,
    e.transaction_sequence,
    e.running_total,
    e.days_since_last_transaction,
    e.amount_vs_avg,
    e.is_high_value,
    current_timestamp as created_at
from enriched e
left join customers c on e.customer_id = c.customer_id
