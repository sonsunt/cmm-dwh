with distinct_currency as (
    select distinct currency
    from {{ ref("stg_bq_coinmarketbase__currency") }}
)

select row_number() over () as id,
        currency
from distinct_currency