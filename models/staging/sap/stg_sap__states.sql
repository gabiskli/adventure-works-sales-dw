with 
    src_state as (
        select
            cast(stateprovinceid as int) as pk_state_code
            , cast(countryregioncode as string) as fk_country_code
            , cast(stateprovincecode as string) as state_code 
            , cast(name as string) as state_name
            --ISONLYSTATEPROVINCEFLAG this columns wil not be used
            --TERRITORYID
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'stateprovince') }}
    )
select *
from src_state