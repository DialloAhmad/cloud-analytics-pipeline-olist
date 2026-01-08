--- models/staging/stg_customers.sql

with source as (
    select * from {{ source('olist_raw', 'RAW_CUSTOMERS') }}
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        customer_city,
        customer_state
    from source
)

select * from renamed