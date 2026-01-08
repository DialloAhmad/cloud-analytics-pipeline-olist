-- Surveillance des déménagements (chamgenent d'adresse) des vendeurs


{% snapshot sellers_snapshot %}

{{
    config(
      unique_key='seller_id',
      check_cols=['seller_city', 'seller_state', 'seller_zip_code_prefix'], 
    )
}}

select * from {{ source('olist_raw', 'RAW_SELLERS') }}

{% endsnapshot %}