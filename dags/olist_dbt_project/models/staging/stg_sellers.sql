--- models/staging/stg_sellers.sql

with source as (
    select * from {{ source('olist_raw', 'RAW_SELLERS') }}
),

renamed as (
    select
        seller_id,
        seller_zip_code_prefix as seller_zip_code,
        seller_city,
        seller_state
    from source
)

select * from renamed