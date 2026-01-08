-- Surveillance des changements d'adresse des clients

{% snapshot customer_snapshot %}

{{
    config(
      unique_key='customer_unique_id',
      check_cols=['customer_city', 'customer_state', 'customer_zip_code_prefix'], 
    )
}}

select * from {{ source('olist_raw', 'RAW_CUSTOMERS') }}

-- On garde uniquement la première occurrence de chaque customer_unique_id
-- pour capturer l'adresse initiale du client
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY customer_zip_code_prefix) = 1

{% endsnapshot %}