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
            cities.PK_ADDRESS as pk_territory
            , cities.CITY_NAME
            , states.STATE_NAME 
            , states.STATE_CODE
            , countries.PK_COUNTRY_CODE as country_code
            , countries.COUNTRY_NAME
        from cities
        left join states
            on cities.fk_state_code = states.pk_state_code
        left join countries
            on states.fk_country_code = countries.pk_country_code
    )
select *
from joined