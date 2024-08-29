with 
    sales as (
        select *
        from {{ ref('fct_sales') }}
    )
    , territories as (
        select *
        from {{ ref('dim_territories') }}
    )
    , customers as (
        select *
        from {{ ref('dim_customers') }}
    )
    , products as (
        select *
        from {{ ref('dim_products') }}
    )
    , joined as (
        select *
        from sales
        left join territories
            on sales.fk_territory = territories.pk_territory
        left join customers
            on sales.fk_customer = customers.pk_customer
        left join products
            on sales.fk_product = products.pk_product
    )
    , aggregation as (
        select
            date_trunc(dt_order, month) as date_month
            , country_name as country
            , customer_name as store
            , product_name as product
            , case
                when customer_type = 'Store' then sum(quantity) 
                when customer_type = 'Online' then 0
            end as units_sold
        from joined
        group by country, store, product, date_month, customer_type
    )
select *
from aggregation