with
    reason as (
        select *
        from {{ ref('stg_sap__sales_reason') }}
    )
    , order_reason as (
        select *
        from {{ ref('stg_sap__sales_order_reason') }}
    )
    , joined as (
        select
            reason.PK_REASON
            , order_reason.FK_ORDER 
            , reason.REASON_NAME
            , reason.REASON_TYPE
            , row_number() over (partition by order_reason.fk_order order by reason.pk_reason) as num_rows 
        from order_reason
        left join reason
            on order_reason.fk_reason = reason.pk_reason
    )
    , deduplicate as (
        select
            FK_ORDER
            , case 
                when pk_reason in (1, 2) then 'Price'
                when pk_reason in (3,4,7,8) then 'Marketing'
                when pk_reason in (5, 9) then 'Quality'
                when pk_reason in (6, 10) then 'Other'
            end as reason_name --regrouping reason in fewer categories.
        from joined
        where num_rows = 1
    )
select *
from deduplicate