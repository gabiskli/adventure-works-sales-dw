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
            int_sales.pk_order_detail
            , int_sales.fk_customer
            , int_sales.fk_territory
            , int_sales.fk_product
            , int_sales.fk_vendor
            , int_sales.fk_order
            , int_sales.dt_order
            , int_sales.status
            , int_sales.order_type
            , int_sales.card_type
            , case 
                when int_sales_reason.reason_name is null then 'Not informed'
                else int_sales_reason.reason_name
            end as reason_name
            , int_sales.unit_price
            , int_sales.discount
            , int_sales.quantity
            , int_sales.gross_sales
            , int_sales.net_sales
            , int_sales.total_sales
            , int_sales.ticket
            , int_sales.discount_category
        from int_sales
        left join int_sales_reason
            on int_sales.fk_order = int_sales_reason.fk_order
    )
select *
from joined