select  price_updated_time,
        extract(date from price_updated_time) as date,
        extract(year from price_updated_time) as year,
        extract(month from price_updated_time) as month,
        extract(day from price_updated_time) as day,
        extract(dayofweek from price_updated_time) as dayofweek,
        case extract(dayofweek from price_updated_time)
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end as dayofweek_name,
        extract(hour from price_updated_time) as hour
from {{ source('bq_coinmarketbase', 'latest_prices') }}