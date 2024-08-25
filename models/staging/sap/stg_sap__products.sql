with 
    src_products as (
        select
            cast(productid as int) as pk_product
            , cast(productsubcategoryid as int) as fk_product_subcategory
            , cast(name as string) as product_name
            , cast(class as string) as product_class
            , cast(style as string) as product_style
            , case
                when sellenddate is not null or discontinueddate is not null then true
                when sellenddate is null or discontinueddate is null then false
            end as is_discontinued
            , cast(productline as string) as product_line
            --MAKEFLAG  -- this lines won't be used
            --PRODUCTNUMBER
            --MODIFIEDDATE
            --FINISHEDGOODSFLAG
            --COLOR
            --SAFETYSTOCKLEVEL
            --REORDERPOINT
            --STANDARDCOST
            --LISTPRICE
            --SIZE
            --SIZEUNITMEASURECODE
            --WEIGHTUNITMEASURECODE
            --WEIGHT
            --DAYSTOMANUFACTURE
            --PRODUCTMODELID
            --SELLSTARTDATE
            --SELLENDDATE
            --DISCONTINUEDDATE
            -- ROWGUID
        from {{ source('sap', 'product') }}
        where finishedgoodsflag = True --Selects only products that can be sold by Adventure Works.
    )
select *
from src_products