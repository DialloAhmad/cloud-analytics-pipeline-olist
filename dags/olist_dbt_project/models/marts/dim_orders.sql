-- Dimensional table for Orders with enhanced logistics features

{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with orders as (
    -- On s'assure de récupérer _ingested_at pour l'audit et les filtres incrémentaux
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    order_status,
    customer_id,
    order_purchase_at,
    order_approved_at,
    order_delivered_carrier_at,
    order_delivered_customer_at,
    order_estimated_delivery_at,
    
    -- Feature Engineering
    datediff(hour, order_purchase_at, order_approved_at) as approval_time_hours,
    datediff(day, order_approved_at, order_delivered_customer_at) as delivery_time_days,
    case 
        when order_delivered_customer_at > order_estimated_delivery_at then true 
        else false 
    end as is_delayed,

    -- Audit pour ingestion incrémentale
    _ingested_at

from orders

{% if is_incremental() %}
  -- On ne traite que les commandes arrivées dans RAW/STG 
  -- APRES la dernière mise à jour de cette table dim_orders
  WHERE _ingested_at > (select max(_ingested_at) from {{ this }} as destination)
{% endif %}