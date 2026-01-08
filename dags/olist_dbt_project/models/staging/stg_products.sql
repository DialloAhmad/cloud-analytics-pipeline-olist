--- models/staging/stg_products.sql

with source as (
    select * from {{ source('olist_raw', 'RAW_PRODUCTS') }}
),

translation as (
    -- Appelle le seed pour les traductions
    select * from {{ ref('product_category_name_translation') }}
),

renamed as (
    select
        source.product_id,
        
        -- On prend la version anglaise si elle existe, sinon la portugaise, sinon 'unknown'
        coalesce(translation.product_category_name_english, source.product_category_name, 'unknown') as category_name,
        
        source.product_name_lenght::int as name_length,
        source.product_description_lenght::int as description_length,
        source.product_photos_qty::int as photos_qty,
        source.product_weight_g::int as weight_g,
        source.product_length_cm::int as length_cm,
        source.product_height_cm::int as height_cm,
        source.product_width_cm::int as width_cm

    from source
    
    -- Left join pour ne pas perdre de produits si la traduction manque
    left join translation 
        on source.product_category_name = translation.product_category_name
)

select * from renamed