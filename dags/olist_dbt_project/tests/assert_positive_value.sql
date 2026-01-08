-- vérification que les valeurs des prix et des frais de livraison sont positives

select
    order_id,
    price,
    freight_value
from {{ ref('stg_order_items') }}
where price < 0 OR freight_value < 0