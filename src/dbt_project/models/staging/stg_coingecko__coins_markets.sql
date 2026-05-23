with source as (
    select * from `workspace`.`raw`.`coingecko_coins_markets`
)

select
    id                                                as coin_id,
    symbol                                            as symbol,
    name                                              as coin_name,
    try_cast(current_price as decimal(38, 8))         as current_price_usd,
    try_cast(null as decimal(38, 2))                  as market_cap_usd,
    try_cast(total_volume as decimal(38, 2))          as total_volume_usd,
    try_cast(price_change_percentage_24h as decimal(18, 8)) as price_change_pct_24h,
    try_cast(market_cap_rank as int)                  as market_cap_rank,
    try_cast(ath as decimal(38, 8))                   as all_time_high_usd,
    try_cast(last_updated as timestamp)               as last_updated_at
from source
where id is not null