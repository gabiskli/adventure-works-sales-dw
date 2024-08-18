/*  
    This test guarantees that 2011 sales are correct
    acording to the accountants
    Este teste garante que as vendas de 2012 estão
    corretas com o valor auditado da contabilidade.
*/

{{
    config(
        severity = 'error'
    )
}}

with
    sales_in_2011 as (
        select sum(gross_sales) as total_sold
        from {{ ref('fct_sales') }}
        where dt_order between '2011-01-01' and '2011-12-31'
    ) -- R$ 12.646.112,16

select total_sold
from sales_in_2011
where total_sold not between 12646112.11 and 12646112.21