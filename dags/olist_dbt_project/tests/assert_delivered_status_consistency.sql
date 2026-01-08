-- Vérification de la cohérence entre le statut "delivered" et la date de livraison


select 
    order_id, 
    order_status, 
    order_delivered_customer_at
from {{ ref('stg_orders') }}
where order_status = 'delivered' 
  and order_delivered_customer_at is null