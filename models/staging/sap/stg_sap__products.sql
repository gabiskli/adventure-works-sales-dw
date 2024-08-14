with 
    src_products as (
        select
            cast(PRODUCTID as int) as pk_product
            , cast(PRODUCTSUBCATEGORYID as int) as fk_product_subcategory
            , cast(NAME as string) as product_name
            , cast(CLASS as string) as product_class
            , cast(STYLE as string) as product_style
            , case
                when DISCONTINUEDDATE is null or SELLENDDATE is null then false
                when DISCONTINUEDDATE is not null or SELLENDDATE is not null then true
                else null
            end as is_discontinued
            , case
                when cast (PRODUCTLINE as string) is null then 'O' -- Indicate "Other", products that are not bicycles.
                else PRODUCTLINE
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
        where FINISHEDGOODSFLAG = True --Selects only products that can be sold by Adventure Works.
    )
select *
from src_products