{% snapshot coins_snapshot %}

{{
    config(
        target_database="de-zoomcamp-400010",
        target_schema="snapshots",
        unique_key="id",

        strategy="check",
        check_cols=["name", "symbol", "currency"]
    )
}}

with latest_period as (
    select max(request_time) as time
    from {{ source('bq_coinmarketbase', 'latest_prices') }}
)

select id, name, symbol, currency
from {{ source('bq_coinmarketbase', 'latest_prices') }}
where request_time = (SELECT time FROM latest_period)

{% endsnapshot %}