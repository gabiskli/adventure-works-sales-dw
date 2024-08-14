with 
    dim_product as (
        select *
        from {{ ref('dim_products') }}
    )
    , dim_customers as (
        select *
        from {{ ref('dim_customers') }}
    )
    , dim_territories as (
        select *
        from {{ ref('dim_territories') }}
    )
    , dim_vendors as (
        select *
        from {{ ref('dim_vendors') }}
    )
    , int_sales_reason as (
        select *
        from {{ ref('int_commercial__sales_reason') }}
    )
    , int_sales as (
        select *
        from {{ ref('int_commercial__sales_order_detail') }}
    )
    , joined as (
        select
            int_sales.PK_ORDER_DETAIL
            , int_sales.FK_CUSTOMER
            , int_sales.FK_TERRITORY
            , int_sales.FK_PRODUCT
            , int_sales.DT_ORDER
            , int_sales.DT_DUE
            , int_sales.STATUS
            , int_sales.order_type
            , int_sales.CARD_TYPE
            , case 
                when int_sales_reason.REASON_NAME is null then 'Not informed'
                else int_sales_reason.REASON_NAME
            end as REASON_NAME
            , int_sales.UNIT_PRICE
            , int_sales.DISCOUNT
            , int_sales.QUANTITY
            , int_sales.TOTAL_SOLD
            , int_sales.NET_TOTAL_SOLD
            , int_sales.PRORATED_FREIGHT
            , int_sales.PRORATED_TAXES
            , int_sales.sales_taxes_freight
            , int_sales.TICKET
            , int_sales.expected_lead_time
        from int_sales
        left join int_sales_reason
            on int_sales.fk_order = int_sales_reason.fk_order
    )
select *
from joined
where reason_name is null