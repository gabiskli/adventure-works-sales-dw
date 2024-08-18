with 
    src_sales_order_reason as (
        select
        cast(salesorderid as int) as fk_order
        , cast(salesreasonid as int) as fk_reason
        --MODIFIEDDATE
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )
select *
from src_sales_order_reason