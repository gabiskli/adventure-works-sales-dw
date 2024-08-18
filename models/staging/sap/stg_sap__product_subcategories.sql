with 
    src_product_subcategories as (
        select 
            cast(productsubcategoryid as int) as pk_product_subcategory
            , cast(productcategoryid as int) as fk_product_category
            , cast(name as string) as subcategory_name
            --MODIFIEDDATE will not be needed
            --ROWGUID 
        from {{ source('sap', 'productsubcategory') }}
    )
select *
from src_product_subcategories