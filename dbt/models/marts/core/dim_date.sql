with distinct_date_hour as (
    select distinct *
    from {{ ref("stg_bq_coinmarketbase__date") }}
),
holidays as (
    select *
    from {{ ref("stg_bq_coinmarketbase__holidays") }}
)

select  distinct_date_hour.price_updated_time as datetime,
        distinct_date_hour.date,
        distinct_date_hour.year,
        distinct_date_hour.month,
        distinct_date_hour.day,
        distinct_date_hour.dayofweek,
        distinct_date_hour.dayofweek_name,
        distinct_date_hour.hour,
        case
            when holidays.date is not null then true
            else false
        end as is_thai_holiday,
        case
            when holidays.description is not null then holidays.description
            else 'N/A'
        end as holiday_name
from distinct_date_hour
left join holidays on holidays.date = distinct_date_hour.date