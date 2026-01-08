-- Fact Table des Ventes (orders)

{{ config(
    materialized='incremental',
    unique_key='sale_sk' 
) }}

with items as (
    -- On récupère _ingested_at pour l'audit et les filtres incrémentaux
    select 
        *, 
        _ingested_at as item_ingested_at 
    from {{ ref('stg_order_items') }}
    
    -- Mode incrémental, on ne charge que les nouveaux items dès le début

    {% if is_incremental() %}
      WHERE _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

products as (
    select product_id from {{ ref('dim_products') }}
),

sellers as (
    select seller_id from {{ ref('dim_sellers') }}
),

orders as (
    select order_id, customer_id, order_purchase_at 
    from {{ ref('dim_orders') }}
),

dim_date as (
    select date_id from {{ ref('dim_date') }}
),

customer_mapping as (
    select customer_id, customer_unique_id 
    from {{ ref('stg_customers') }}
),

final_customers as (
    select customer_unique_id from {{ ref('dim_customers') }}
)

select
    md5(items.order_id || '-' || items.order_item_id) as sale_sk,

    items.order_id,
    items.product_id,
    items.seller_id,
    customer_mapping.customer_unique_id,
    to_date(orders.order_purchase_at) as date_id,

    items.price,
    items.freight_value,
    (items.price + items.freight_value) as total_amount,

    -- ingestion dans la fact pour le prochain run
    items.item_ingested_at as _ingested_at

from items

inner join orders 
    on items.order_id = orders.order_id

inner join products 
    on items.product_id = products.product_id

inner join sellers 
    on items.seller_id = sellers.seller_id

inner join customer_mapping
    on orders.customer_id = customer_mapping.customer_id

inner join final_customers
    on customer_mapping.customer_unique_id = final_customers.customer_unique_id

inner join dim_date
    on to_date(orders.order_purchase_at) = dim_date.date_id