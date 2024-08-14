with 
    src_credit_cards as (
        select
            cast(CREDITCARDID as int) as pk_credit_card
            , cast(CARDTYPE as string) as card_type
            --CARDNUMBER these columns won't be used
            --EXPMONTH
            --EXPYEAR
            --MODIFIEDDATE
        from {{ source('sap', 'creditcard') }}
    )
select *
from src_credit_cards