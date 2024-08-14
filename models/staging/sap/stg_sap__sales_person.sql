with 
    src_sales_person as (
        select
            cast(BUSINESSENTITYID as int) as pk_vendor
            , cast(SALESQUOTA as numeric(18,2)) as expected_yearly_sales
            , cast(BONUS as numeric(18,2)) as vendor_bonus
            , cast(COMMISSIONPCT as numeric(18,2)) as commission_pct
            , cast(SALESYTD as numeric(18,2)) as vendor_sales_ytd
            , cast(SALESLASTYEAR as numeric(18,2)) as vendor_sales_last_year
            --MODIFIEDDATE
            --TERRITORYID
            --ROWGUID

        from {{ source('sap', 'salesperson') }}
    )
select *
from src_sales_person