with 
    src_stores as (
        select
            cast(businessentityid as int) as pk_store
            , cast(name as string) as store_name
            --SALESPERSONID these columns will not be used
            --DEMOGRAPHICS
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'store') }}
    )
select *
from src_stores