with 
    cities as (
        select *
        from {{ ref('stg_sap__addresses') }}
    )
    , states as (
        select *
        from {{ ref('stg_sap__states') }}
    )
    , countries as (
        select *
        from {{ ref('stg_sap__countries') }}
    )
    , joined as (
        select
            cities.pk_address as pk_territory
            , cities.city_name as city_name
            , states.state_name as state_name
            , states.state_code as state_code
            , countries.pk_country_code as country_code
            , countries.country_name as country_name
        from cities
        left join states
            on cities.fk_state_code = states.pk_state_code
        left join countries
            on states.fk_country_code = countries.pk_country_code
    )
select *
from joined