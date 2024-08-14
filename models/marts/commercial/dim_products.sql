with 
    products as (
        select *
        from {{ ref('stg_sap__products') }}
    )
    , subcategories as (
        select *
        from {{ ref('stg_sap__product_subcategories') }}
    )
    , categories as (
        select *
        from {{ ref('stg_sap__product_categories') }}
    )
    , join_categories as (
        select
            subcategories.PK_PRODUCT_SUBCATEGORY
            , categories.PK_PRODUCT_CATEGORY as fk_product_category
            , categories.CATEGORY_NAME
        from subcategories
        left join categories
            on subcategories.fk_product_category = categories.pk_product_category
    )
    , joined as (
        select 
            products.PK_PRODUCT
            , products.PRODUCT_NAME
            , case
                when join_categories.category_name is null then 'Other'
                else join_categories.category_name
            end as product_category
            , products.IS_DISCONTINUED
        from products
        left join join_categories
            on products.fk_product_subcategory = join_categories.pk_product_subcategory
    )
select *
from joined