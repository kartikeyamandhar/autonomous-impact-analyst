with top_exchanges as (
    select * from {{ ref('stg_coingecko__exchanges') }}
    where trust_score_rank is not null and trust_score_rank <= 10
),

top_coins as (
    select * from {{ ref('stg_coingecko__coins_markets') }}
    where market_cap_rank is not null and market_cap_rank <= 20
)

select
    {{ dbt_utils.generate_surrogate_key(['e.exchange_id', 'c.coin_id']) }} as coverage_id,
    e.exchange_id,
    e.exchange_name,
    e.trust_score,
    c.coin_id,
    c.symbol,
    c.coin_name,
    c.market_cap_usd
from top_exchanges e
cross join top_coins c
