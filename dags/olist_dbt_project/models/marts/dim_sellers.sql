-- Dimensional table for sellers

{{ config(materialized='table') }}

with sellers as (
    select * from {{ ref('stg_sellers') }}
)

select
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state
from sellers