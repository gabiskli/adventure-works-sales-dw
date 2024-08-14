with 
    src_sales_order_reason as (
        select
        cast(SALESORDERID as int) as fk_order
        , cast(SALESREASONID as int) as fk_reason
        --MODIFIEDDATE
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )
select *
from src_sales_order_reason