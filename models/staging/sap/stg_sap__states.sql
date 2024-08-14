with 
    src_state as (
        select
            cast(STATEPROVINCEID as int) as pk_state_code
            , cast(COUNTRYREGIONCODE as string) as fk_country_code
            , cast(STATEPROVINCECODE as string) as state_code 
            , cast(NAME as string) as state_name
            --ISONLYSTATEPROVINCEFLAG this columns wil not be used
            --TERRITORYID
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'stateprovince') }}
    )
select *
from src_state