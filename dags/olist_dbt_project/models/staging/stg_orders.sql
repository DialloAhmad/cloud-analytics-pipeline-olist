--- models/staging/stg_orders.sql

with source as (
    select * from {{ source('olist_raw', 'RAW_ORDERS') }}
),

 renamed as (
    select
        order_id,
        customer_id,
        order_status,
        -- Conversion des dates (renvoie NULL si format invalide)
        try_to_timestamp(order_purchase_timestamp) as order_purchase_at,
        try_to_timestamp(order_approved_at) as order_approved_at,
        try_to_timestamp(order_delivered_carrier_date) as order_delivered_carrier_at,
        try_to_timestamp(order_delivered_customer_date) as order_delivered_customer_at,
        try_to_timestamp(order_estimated_delivery_date) as order_estimated_delivery_at,
        _ingested_at -- Audit auto ingestion timestamp
    from source
)

select * from renamed
-- On ne garde que les commandes avec une cohérence entre le statut "delivered" et la date de livraison
where not (order_status = 'delivered' and order_delivered_customer_at is null)