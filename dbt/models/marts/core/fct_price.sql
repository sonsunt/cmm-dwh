select  dim_coins.id as coin_detail_id,
        dim_date.datetime as latest_prices_datetime,
        dim_currency.id as currency_id,
        src.price,
        src.market_cap
from {{ ref("stg_bq_coinmarketbase__price")}} as src
left join {{ ref("dim_coins") }} as dim_coins
    on dim_coins.coin_id = src.coin_id
    and dim_coins.name = src.name
    and dim_coins.symbol = src.symbol
left join {{ ref("dim_date") }} as dim_date
    on dim_date.datetime = src.price_date
left join {{ ref("dim_currency") }} as dim_currency
    on dim_currency.currency = src.currency