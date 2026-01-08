-- models/staging/stg_payments.sql

with source as (
    select * from {{ source('olist_raw', 'RAW_PAYMENTS') }}
),

renamed as (
    select
        order_id,
        payment_sequential::int as payment_sequential, 
        payment_type,
        payment_installments::int as payment_installments, 
        payment_value::float as payment_value
    from source
)

select * from renamed
-- On ne garde que les paiements avec un nombre d'échéances valide
where payment_installments >= 1
--  On ne garde que les paiements dont l'order_id existe encore dans stg_orders nettoyé
AND order_id in (select order_id from {{ ref('stg_orders') }})