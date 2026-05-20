with token_profiles as (
    select * from {{ ref('int_token_profiles') }}
),

whale_activity as (
    select * from {{ ref('int_whale_activity') }}
)

select
    tp.coin_id,
    tp.symbol,
    tp.coin_name,
    tp.current_price_usd,
    tp.market_cap_usd,
    tp.total_volume_usd,
    tp.market_cap_rank,
    tp.price_change_pct_24h,
    tp.all_time_high_usd,
    tp.protocol_id,
    tp.protocol_name,
    tp.protocol_tvl_usd,
    coalesce(wa.tx_count, 0)             as whale_tx_count,
    coalesce(wa.total_value, 0)          as whale_total_value,
    coalesce(wa.unique_counterparties, 0) as whale_unique_counterparties
from token_profiles tp
left join whale_activity wa on tp.contract_address = wa.address
