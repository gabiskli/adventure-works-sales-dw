with 
    src_sales_person as (
        select
            cast(BUSINESSENTITYID as int) as pk_vendor
            , cast(SALESQUOTA as numeric) as expected_yearly_sales
            , cast(BONUS as numeric) as vendor_bonus
            , cast(COMMISSIONPCT as numeric) as commission_pct
            , cast(SALESYTD as numeric) as vendor_sales_ytd
            , cast(SALESLASTYEAR as numeric) as vendor_sales_last_year
            --MODIFIEDDATE
            --TERRITORYID
            --ROWGUID

        from {{ source('sap', 'salesperson') }}
    )
select *
from src_sales_person