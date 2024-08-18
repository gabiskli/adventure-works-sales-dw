with 
    src_sales_orders as (
        select
            cast(salesorderid as int) as pk_order
            , cast(billtoaddressid as int) as fk_territory
            , cast(creditcardid as int) as fk_card
            , cast(customerid as int) as fk_customer
            , case
                when cast(salespersonid as int) is null then 0
                else cast(salespersonid as int)
            end as fk_vendor
            , cast(cast(orderdate as datetime) as date) as dt_order
            , cast(cast(duedate as datetime) as date) as dt_due
            , cast(taxamt as numeric) as tax_amount
            , cast(freight as numeric) as freight
            , case
                when status = 1 then 'In progress'
                when status = 2 then 'Approved'
                when status =3 then 'Backordered'
                when status = 4 then 'Rejected'
                when status = 5 then 'Shipped'
                when status = 6 then 'Cancelled'
                else null
            end as status
            , case 
                when onlineorderflag = true then 'Online'
                when onlineorderflag = false then 'Vendor'
            end as order_type
            --REVISIONNUMBER
            --SHIPDATE
            --PURCHASEORDERNUMBER
            --ACCOUNTNUMBER
            --TERRITORYID
            --SHIPTOADDRESSID
            --SHIPMETHODID
            --CREDITCARDAPPROVALCODE
            --CURRENCYRATEID
            --COMMENT
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'salesorderheader') }}
    )
select *
from src_sales_orders