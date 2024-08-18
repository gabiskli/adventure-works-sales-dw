with 
    src_sales_person as (
        select
            cast(businessentityid as int) as pk_vendor
            , cast(salesquota as numeric) as expected_yearly_sales
            , cast(bonus as numeric) as vendor_bonus
            , cast(commissionpct as numeric) as commission_pct
            , cast(salesytd as numeric) as vendor_sales_ytd
            , cast(saleslastyear as numeric) as vendor_sales_last_year
            --MODIFIEDDATE
            --TERRITORYID
            --ROWGUID

        from {{ source('sap', 'salesperson') }}
    )
select *
from src_sales_person