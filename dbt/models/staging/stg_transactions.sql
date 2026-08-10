with source as (
    select * from staging.transactions
)
select
    transaction_id,
    customer_id,
    transaction_type,
    amount,
    timestamp as transaction_timestamp,
    merchant_name,
    merchant_category,
    location_city,
    location_state,
    is_weekend,
    hour as transaction_hour,
    is_online,
    is_fraud,
    ingestion_timestamp
from source
