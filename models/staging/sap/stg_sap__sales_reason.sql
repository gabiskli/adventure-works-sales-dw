with 
    src_sales_reason as (
        select
        cast(salesreasonid as int) as pk_reason
        , cast(name as string) as reason_name
        , cast(reasontype as string) as reason_type
        --MODIFIEDDATE
        from {{ source('sap', 'salesreason') }}
    )
select *
from src_sales_reason