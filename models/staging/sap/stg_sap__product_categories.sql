with 
    src_product_categories as (
        select
            cast(PRODUCTCATEGORYID as int) as pk_product_category
            , cast(NAME as varchar) as category_name
            --MODIFIEDDATE
            --ROWGUID
        from {{ source('sap', 'productcategory') }}
    )
select *
from src_product_categories