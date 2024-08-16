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
            prorated_taxes + prorated_freight+ total_sold as total_received
            , sales_taxes_freight
        from {{ ref('fct_sales') }}
    ) -- Expected that both values are the same

select
    total_received
    , sales_taxes_freight
from profit
where 
    sales_taxes_freight > (1.05 * total_received) 
    or sales_taxes_freight < (0.95 * total_received)