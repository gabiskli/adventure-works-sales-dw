with 
    src_sales_orders_detail as (
        select
            cast(SALESORDERDETAILID  as int) as pk_order_detail
            , cast(SALESORDERID as int) as fk_order
            , cast(PRODUCTID as int) as fk_product
            , cast(ORDERQTY as int) as quantity
            , cast(UNITPRICE as numeric) as unit_price
            , cast(UNITPRICEDISCOUNT as numeric) as discount
            --CARRIERTRACKINGNUMBER
            --SPECIALOFFERID
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'salesorderdetail') }}
    )
select *
from src_sales_orders_detail