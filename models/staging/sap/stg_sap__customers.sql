with 
    src_customers as (
        select
            cast(customerid as int) as pk_customer
            , cast(personid as int) as fk_person
            , cast(storeid as int) as fk_store
            --TERRITORYID
            --ROWGUID
            --MODIFIEDDATE
        from {{ source('sap', 'customer') }}
        where personid is not null -- Excluding duplicated values in STOREID field.
    )
select *
from src_customers