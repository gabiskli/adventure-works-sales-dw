with 
    src_product_categories as (
        select
            cast(productcategoryid as int) as pk_product_category
            , cast(name as string) as category_name
            --MODIFIEDDATE
            --ROWGUID
        from {{ source('sap', 'productcategory') }}
    )
select *
from src_product_categories