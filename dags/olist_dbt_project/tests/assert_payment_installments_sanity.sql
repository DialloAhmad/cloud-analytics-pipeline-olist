-- Vérification de la logique de paiement, on ne peut pas payer en 0 fois ou en -1 fois

select 
    order_id, 
    payment_installments
from {{ ref('stg_payments') }}
where payment_installments < 1