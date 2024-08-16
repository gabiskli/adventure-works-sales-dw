with 
    src_sales_orders as (
        select
            cast(SALESORDERID as int) as pk_order
            , cast(BILLTOADDRESSID as int) as fk_territory
            , cast(CREDITCARDID as int) as fk_card
            , cast(CUSTOMERID as int) as fk_customer
            , case
                when cast(SALESPERSONID as int) is null then 0
                else cast(SALESPERSONID as int)
            end as fk_vendor
            , cast(cast(ORDERDATE as datetime) as date) as dt_order
            , cast(cast(DUEDATE as datetime) as date) as dt_due
            , cast(SUBTOTAL as numeric) as gross_profit
            , cast(TAXAMT as numeric) as tax_amount
            , cast(FREIGHT as numeric) as freight
            , cast(TOTALDUE as numeric) as net_profit
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
                when ONLINEORDERFLAG = true then 'Online'
                when ONLINEORDERFLAG = false then 'Vendor'
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