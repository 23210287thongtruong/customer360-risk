{{ config(
    materialized='table'
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    -- We can use a hash for surrogate key in dbt
    md5(customer_id) as customer_key,
    customer_id,
    name,
    date_of_birth,
    extract(year from age(current_date, date_of_birth)) as age,
    address || ', ' || city || ', ' || state || ', ' || zip_code as full_address,
    city,
    state,
    zip_code,
    phone,
    email,
    annual_income,
    case
        when annual_income < 30000 then 'Low Income'
        when annual_income < 50000 then 'Lower Middle'
        when annual_income < 75000 then 'Middle'
        when annual_income < 100000 then 'Upper Middle'
        else 'High Income'
    end as income_bracket,
    job_title,
    employment_status,
    marital_status,
    customer_since,
    current_date - customer_since as customer_tenure_days,
    case
        when email is not null and name is not null and customer_id is not null then true
        else false
    end as is_complete_record,
    true as is_active,
    current_timestamp as created_at,
    current_timestamp as updated_at
from customers
