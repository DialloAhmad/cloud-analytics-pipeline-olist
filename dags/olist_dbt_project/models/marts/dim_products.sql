-- Dimensional table for products with additional feature engineering

{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
     -- Mise en forme des noms de catégories
    -- Ex: 'health_beauty' devient 'Health Beauty'
    initcap(replace(category_name, '_', ' ')) as category_name,
    name_length,
    description_length,
    photos_qty,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    -- Feature Engineering : Volume du colis
    (length_cm * height_cm * width_cm) as product_volume_cm3
from products