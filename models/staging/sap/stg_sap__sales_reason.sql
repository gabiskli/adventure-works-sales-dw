with 
    src_sales_reason as (
        select
        cast(SALESREASONID as int) as pk_reason
        , cast(NAME as string) as reason_name
        , cast(REASONTYPE as string) as reason_type
        --MODIFIEDDATE
        from {{ source('sap', 'salesreason') }}
    )
select *
from src_sales_reason