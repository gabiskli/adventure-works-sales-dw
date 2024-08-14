with 
    src_product_subcategories as (
        select 
            cast(PRODUCTSUBCATEGORYID as int) as pk_product_subcategory
            , cast(PRODUCTCATEGORYID as int) as fk_product_category
            , cast(NAME as string) as subcategory_name
            --MODIFIEDDATE will not be needed
            --ROWGUID 
        from {{ source('sap', 'productsubcategory') }}
    )
select *
from src_product_subcategories