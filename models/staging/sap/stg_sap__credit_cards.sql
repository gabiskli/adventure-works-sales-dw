with 
    src_credit_cards as (
        select
            cast(creditcardid as int) as pk_credit_card
            , cast(cardtype as string) as card_type
            --CARDNUMBER these columns won't be used
            --EXPMONTH
            --EXPYEAR
            --MODIFIEDDATE
        from {{ source('sap', 'creditcard') }}
    )
select *
from src_credit_cards