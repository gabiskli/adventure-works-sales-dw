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
            , sum(quantity) as units_sold
        from joined
        where customer_type = 'Store' and is_discontinued = False
        group by date_month, product, store, country
    )
select *
from aggregation
