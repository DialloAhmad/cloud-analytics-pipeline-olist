-- models/staging/stg_order_items.sql

with source as (
    select * from {{ source('olist_raw', 'RAW_ITEMS') }}
),

renamed as (
    select
        order_id,
        order_item_id::int as order_item_id,
        product_id,
        seller_id,
        try_to_timestamp(shipping_limit_date) as shipping_limit_at,
        price::float as price,
        freight_value::float as freight_value,
        _ingested_at -- Audit auto ingestion timestamp
    from source
)

select * from renamed
--  On ne garde que les items dont l'order_id existe encore dans stg_orders nettoyé
where order_id in (select order_id from {{ ref('stg_orders') }})