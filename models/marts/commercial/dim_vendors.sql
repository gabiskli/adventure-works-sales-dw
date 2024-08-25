with 
    sales_person as (
        select *
        from {{ ref('stg_sap__sales_person') }}
    )
    , people as (
        select *
        from {{ ref('stg_sap__people') }}
    )
    , joined as (
        select
            sales_person.pk_vendor
            , people.person_name as vendor_name
            , sales_person.expected_yearly_sales as vendor_sales_goal
            , sales_person.vendor_sales_ytd as vendor_sales
            , case 
                when sales_person.expected_yearly_sales is null and sales_person.vendor_sales_ytd is not null then True
                when sales_person.expected_yearly_sales > sales_person.vendor_sales_ytd then False
                when sales_person.expected_yearly_sales < sales_person.vendor_sales_ytd then True
            end as is_vendor_goal_met
        from sales_person
        left join people
            on sales_person.pk_vendor = people.pk_people
    )
select *
from joined