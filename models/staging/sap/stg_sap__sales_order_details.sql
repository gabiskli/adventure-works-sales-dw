with 
    src_sales_orders_detail as (
        select
            cast(salesorderdetailid  as int) as pk_order_detail
            , cast(salesorderid as int) as fk_order
            , cast(productid as int) as fk_product
            , cast(orderqty as int) as quantity
            , cast(unitprice as numeric) as unit_price
            , cast(unitpricediscount as numeric) as discount
            --CARRIERTRACKINGNUMBER
            --SPECIALOFFERID
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'salesorderdetail') }}
    )
select *
from src_sales_orders_detail