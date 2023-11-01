with latest_period as (
    select max(request_time) as time
    from {{ source('bq_coinmarketbase', 'latest_prices') }}
)

select id, name, symbol
from {{ source('bq_coinmarketbase', 'latest_prices') }}
where request_time = (SELECT time FROM latest_period)