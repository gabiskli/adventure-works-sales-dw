with 
    src_country as (
        select
            cast(COUNTRYREGIONCODE as string) as pk_country_code
            , cast(NAME as string) as country_name
            -- MODIFIEDDATE this column will not be used
        from {{ source('sap', 'countryregion') }}
    )
select *
from src_country