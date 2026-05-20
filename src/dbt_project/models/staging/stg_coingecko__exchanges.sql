with source as (
    select * from {{ source('coingecko', 'coingecko_exchanges') }}
)

select
    id                                            as exchange_id,
    name                                          as exchange_name,
    try_cast(year_established as int)             as year_established,
    country                                       as country,
    try_cast(trust_score as int)                  as trust_score,
    try_cast(trust_score_rank as int)             as trust_score_rank,
    try_cast(trade_volume_24h_btc as decimal(38, 8)) as trade_volume_24h_btc,
    url                                           as url
from source
where id is not null
