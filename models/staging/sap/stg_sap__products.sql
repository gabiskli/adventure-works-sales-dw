with 
    src_products as (
        select
            cast(productid as int) as pk_product
            , cast(productsubcategoryid as int) as fk_product_subcategory
            , cast(name as string) as product_name
            , cast(class as string) as product_class
            , cast(style as string) as product_style
            , case
                when discontinueddate is null or sellenddate is null then false
                when discontinueddate is not null or sellenddate is not null then true
                else null
            end as is_discontinued
            , case
                when cast (productline as string) is null then 'O' -- Indicate "Other", products that are not bicycles.
                else productline
            end as product_line
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
            -- ROWGUID
        from {{ source('sap', 'product') }}
        where finishedgoodsflag = True --Selects only products that can be sold by Adventure Works.
    )
select *
from src_products