with 
    stores as (
        select *
        from {{ ref('stg_sap__stores') }}
    )
    , people as (
        select *
        from {{ ref('stg_sap__people') }}
    )
    , customers as (
        select *
        from {{ ref('stg_sap__customers') }}
    )
    , rfm as (
        select *
        from {{ ref('int_commercial__rfm_analysis') }}
    )
    , joined as (
        select
            customers.pk_customer
            , customers.fk_person
            , customers.fk_store
            , stores.store_name
            , people.person_name
            , rfm.r_score
            , rfm.f_score
            , rfm.m_score
            , rfm.rfm_segment
        from customers
        left join stores
            on customers.fk_store= stores.pk_store
        left join people
            on customers.fk_person = people.pk_people
        left join rfm
            on customers.pk_customer = rfm.fk_customer
    )
    , categorization as (
        select
            pk_customer
            , case 
                when joined.fk_store is null then joined.person_name
                when joined.fk_store is not null then joined.store_name 
                else joined.person_name
            end as customer_name --joining person and store name in one column.
            , case 
                when fk_store is null then 'Person'
                when fk_store is not null then 'Store'
            end as customer_type
            /*Since there is two types of customers, stores and people then I created a new columns with customer name.
            There are some stores that have also a person associated with it. In this cases I got the person name as the
            customer name, as the store name was already considered when the person fk is null.*/
            , rfm_segment
        from joined
    )
select *
from categorization
