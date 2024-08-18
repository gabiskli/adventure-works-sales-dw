with 
    sales as (
        select *
        from {{ ref('fct_sales') }}
    )
    , territories as (
        select *
        from {{ ref('dim_territories') }}
    )
    , vendors as (
        select *
        from {{ ref('dim_vendors') }}
    )
    , joined as (
        select *
        from sales
        left join territories
            on sales.fk_territory = territories.pk_territory
        left join vendors
            on sales.fk_vendor = vendors.pk_vendor
    )
    , aggregation as (
        select
            vendor_name as vendor 
            , country_name as country
            , avg(net_sales) as avg_total_sold
            , sum(net_sales) as total_sold
        from joined
        where vendor_name is not null --this are sales done online, and won't be considered
        group by country, vendor
    )
select *
from aggregation