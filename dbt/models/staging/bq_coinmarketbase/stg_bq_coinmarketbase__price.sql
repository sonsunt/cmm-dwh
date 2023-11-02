select id as coin_id,
        name,
        symbol, 
        currency, 
        price, 
        market_cap, 
        price_updated_time as price_date
from {{ source('bq_coinmarketbase', 'latest_prices') }}