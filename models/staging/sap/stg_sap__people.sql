with 
    src_people as (
        select 
            cast(businessentityid as int) as pk_people
            , cast(persontype as string) as person_type
            , cast(firstname as string) || ' ' || cast(lastname as string) as person_name
                -- Keep the western starndard name even if the namestyle is eastern.
            -- NAMESTYLE
            --TITLE
            --MIDDLENAME
            --SUFFIX
            --EMAILPROMOTION
            --ADDITIONALCONTACTINFO
            --DEMOGRAPHICS
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'person') }}
    )
select *
from src_people