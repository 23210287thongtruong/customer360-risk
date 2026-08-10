with source as (
    select * from staging.credit_scores
)
select
    customer_id,
    credit_score,
    score_date,
    credit_history_length,
    number_of_accounts,
    total_debt,
    credit_utilization,
    ingestion_timestamp
from source
