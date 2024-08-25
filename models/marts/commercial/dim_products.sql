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
            subcategories.pk_product_subcategory
            , categories.pk_product_category as fk_product_category
            , categories.category_name
        from subcategories
        left join categories
            on subcategories.fk_product_category = categories.pk_product_category
    )
    , joined as (
        select 
            products.pk_product
            , products.product_name
            , case
                when join_categories.category_name is null then 'Other'
                else join_categories.category_name
            end as product_category
            /*, case 
                when products.product_line = "R" then 'Road'
                when products.product_line = "T" then 'Touring'
                when products.product_line = 'M' then 'Mountain'
                when products.product_line = 'S' then 'Standard'
                else 'Other'
            end as product_line_cteg*/
            , product_line
            , products.is_discontinued
        from products
        left join join_categories
            on products.fk_product_subcategory = join_categories.pk_product_subcategory
    )
select *
from joined
