-- Vérification qu'il n'y a pas de produits sans vendeur associé

select distinct seller_id
from {{ ref('fact_sales') }}
where seller_id not in (
  select seller_id from {{ ref('dim_sellers') }}
)