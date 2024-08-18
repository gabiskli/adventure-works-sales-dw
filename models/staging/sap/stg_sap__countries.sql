with 
    src_country as (
        select
            cast(countryregioncode as string) as pk_country_code
            , cast(name as string) as country_name
            -- MODIFIEDDATE this column will not be used
        from {{ source('sap', 'countryregion') }}
    )
select *
from src_country