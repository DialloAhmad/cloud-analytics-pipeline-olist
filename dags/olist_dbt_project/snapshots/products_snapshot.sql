-- Surveillance des changements de catégorie de produits


{% snapshot products_snapshot %}

{{
    config(
      unique_key='product_id',
      check_cols=['product_category_name'],
    )
}}

select * from {{ source('olist_raw', 'RAW_PRODUCTS') }}

{% endsnapshot %}