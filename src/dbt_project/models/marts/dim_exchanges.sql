select
    exchange_id,
    exchange_name,
    year_established,
    country,
    trust_score,
    trust_score_rank,
    trade_volume_24h_btc,
    url
from {{ ref('stg_coingecko__exchanges') }}
