/*  
    This test guarantees that the calculations performed in the 
    fact sales table are correct.
    The test will senda warn if difference in calculations are greater than 5%. 
*/

{{
    config(
        severity = 'warn'
    )
}}

with
    profit as (
        select
            prorated_taxes + prorated_freight+ gross_sales as total_received
            , total_sales
        from {{ ref('int_commercial__sales_order_detail') }}
    ) -- Expected that both values are the same

select
    total_received
    , total_sales
from profit
where 
    total_sales > (1.05 * total_received) 
    or total_sales < (0.95 * total_received)