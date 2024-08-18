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
            order_details.pk_order_detail
            , orders.pk_order as fk_order
            , orders.fk_customer
            , orders.fk_territory
            , orders.fk_vendor
            , order_details.fk_product
            , orders.fk_card
            , orders.dt_order
            , orders.dt_due
            , orders.status
            , orders.order_type
            , credit_card.card_type
            , order_details.unit_price
            , order_details.discount
            , order_details.quantity
            , orders.tax_amount
            , orders.freight
        from order_details
        left join orders
            on order_details.fk_order = orders.pk_order
        left join credit_card
            on orders.FK_CARD = credit_card.pk_credit_card
    )
    , metrics as (
        select
            pk_order_detail
            , fk_order
            , fk_customer
            , fk_territory
            , fk_product
            , fk_vendor
            , dt_order
            , dt_due
            , status
            , order_type
            , case 
                when card_type is null then 'Other'
                else card_type
            end as card_type
            , unit_price
            , discount
            , quantity
            , unit_price * quantity as gross_sales
            , unit_price * (1 - discount) * quantity as net_sales
            , cast(freight / count(*) over (partition by fk_order) as numeric) as prorated_freight
            , cast((tax_amount 
                * (unit_price * quantity)
                / (sum(unit_price * quantity) over (partition by fk_order))) as numeric) 
            as prorated_taxes
            , (unit_price * (1 - discount) * quantity) -- net sales
                + (freight / count(*) over (partition by fk_order)) -- freight
                + (tax_amount * (unit_price * quantity) / (sum(unit_price * quantity) over (partition by fk_order)))
            as total_sales
            , sum(unit_price * (1 - discount) * quantity) over (partition by fk_order) 
            / count(*) over (partition by fk_order) 
            as ticket
            , date_diff(dt_due, dt_order, day) as expected_lead_time
            , case 
                when discount = 0 then 'No discount'
                when discount <= 0.1 then 'Up to 10% discount'
                when discount <= 0.3 then 'From 10% to 30% discount'
                when discount > 0.3 then 'Greater then 30% discount'
            end as discount_category
        from joined
    )
select *
from metrics