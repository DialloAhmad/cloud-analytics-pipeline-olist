-- Dimensional table of customers

{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

unique_customers as (
    select 
        customer_unique_id,
        -- Un client peut avoir plusieurs adresses dans le temps, on va dédoublonner arbitrairement et prendre la dernière
        max(customer_city) as customer_city,
        max(customer_state) as customer_state,
        max(customer_zip_code) as customer_zip_code
    from customers
    group by 1
)

select * from unique_customers