with 
    orders as (
        select *
        from {{ ref('stg_sap__sales_orders') }}
    )
    , order_details as (
        select *
        from {{ ref('stg_sap__sales_order_details') }}
    )
    , joined as (
        select
            order_details.pk_order_detail
            , orders.pk_order as fk_order
            , orders.fk_customer
            , orders.dt_order
            , orders.dt_due
            , order_details.unit_price
            , order_details.discount
            , order_details.quantity
        from order_details
        left join orders
            on order_details.fk_order = orders.pk_order
    )
    , metrics as (
        select
            fk_customer
            , max(dt_order) as last_order_date
            , date_diff((select max(dt_order) from joined), max(dt_order), day) as recency
            , count(distinct(fk_order)) as frequency 
            , sum(unit_price * (1 - discount) * quantity) as monetary_value
        from joined
        group by fk_customer
    )
    , rfm_calculation as (
        select
            *
            , ntile(5) over (order by recency desc) as r_score
            , ntile(5) over (order by frequency, recency desc) as f_score
            , ntile(5) over (order by monetary_value) as m_score
            , cast(cast((ntile(5) over (order by recency desc)) as string)||cast((ntile(5) over (order by frequency, recency desc) )as string)||cast((ntile(5) over (order by monetary_value)) as string) as int) as rfm_score
        from metrics
    )
    , rfm_segmentation as (
        select
            fk_customer
            , rfm_score
            , r_score
            , f_score
            , m_score
            , case 
                when rfm_score = 555 then 'high value'
                when r_score = 1 then 'lost customer'
                when r_score > 2 and f_score > 3 then 'core'
                when r_score > 2 and f_score = 3 then 'promising'
                when r_score = 5 and f_score < 3 then 'new clients'
                when r_score = 2 and f_score > 3 then 'need attention'
                else 'general'
            end rfm_segment
        from rfm_calculation
    )
select *
from rfm_segmentation


