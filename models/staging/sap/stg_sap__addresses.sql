with 
    src_address as (
        select
            cast(addressid as int) as pk_address
            , cast(stateprovinceid as int) as fk_state_code
            , cast(city as string) as city_name
            --SPATIALLOCATION this will not be used
            --POSTALCODE 
            --ADDRESSLINE1
            --ADDRESSLINE2
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'address') }}
    )
select *
from src_address