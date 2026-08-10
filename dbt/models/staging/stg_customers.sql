with source as (
    select * from staging.customers
)
select
    customer_id,
    name,
    date_of_birth,
    address,
    city,
    state,
    zip_code,
    phone,
    email,
    annual_income,
    job_title,
    employment_status,
    marital_status,
    created_date as customer_since,
    ingestion_timestamp
from source
