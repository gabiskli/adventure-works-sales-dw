with 
    orders as (
        select *
        from {{ ref('stg_sap__sales_orders') }}
    )
    , order_details as (
        select *
        from {{ ref('stg_sap__sales_order_details') }}
    )
    , credit_card as (
        select *
        from {{ ref('stg_sap__credit_cards') }}
    )
    , joined as (
        select
            order_details.PK_ORDER_DETAIL
            , orders.PK_ORDER as fk_order
            , orders.FK_CUSTOMER
            , orders.FK_TERRITORY
            , orders.fk_vendor
            , order_details.FK_PRODUCT
            , orders.FK_CARD
            , orders.DT_ORDER
            , orders.DT_DUE
            , orders.STATUS
            , orders.order_type
            , credit_card.card_type
            , order_details.UNIT_PRICE
            , order_details.DISCOUNT
            , order_details.QUANTITY
            , orders.TAX_AMOUNT
            , orders.FREIGHT
        from order_details
        left join orders
            on order_details.fk_order = orders.pk_order
        left join credit_card
            on orders.FK_CARD = credit_card.pk_credit_card
    )
    , metrics as (
        select
            PK_ORDER_DETAIL
            , fk_order
            , FK_CUSTOMER
            , FK_TERRITORY
            , FK_PRODUCT
            , fk_vendor
            , DT_ORDER
            , DT_DUE
            , STATUS
            , order_type
            , case 
                when CARD_TYPE is null then 'Other'
                else CARD_TYPE
            end as CARD_TYPE
            , UNIT_PRICE
            , DISCOUNT
            , QUANTITY
            , unit_price * quantity as total_sold
            , unit_price * (1 - discount) * quantity as net_total_sold
            , cast(freight / count(*) over (partition by fk_order) as numeric) as prorated_freight
            , cast((tax_amount 
                * unit_price 
                * quantity 
                / sum(unit_price * quantity) over (partition by fk_order)) as numeric) 
            as prorated_taxes
            , cast((unit_price 
                * (1 - discount) 
                * quantity)
                + freight / count(*) over (partition by fk_order)
                + tax_amount * unit_price * quantity / sum(unit_price * quantity) over (partition by fk_order) as numeric)
            as sales_taxes_freight
            , sum(unit_price * (1 - discount) * quantity) over (partition by fk_order) 
            / count(*) over (partition by fk_order) 
            as ticket
            , date_diff(dt_due, dt_order, day) as expected_lead_time
        from joined
    )
select *
from metrics