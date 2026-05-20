with protocol_metrics as (
    select * from {{ ref('int_protocol_metrics') }}
),

token_by_symbol as (
    select
        upper(symbol)        as symbol_u,
        current_price_usd,
        price_change_pct_24h,
        market_cap_usd
    from {{ ref('int_token_profiles') }}
    where symbol is not null
    qualify row_number() over (partition by upper(symbol) order by market_cap_usd desc nulls last) = 1
)

select
    pm.protocol_id,
    pm.protocol_name,
    pm.symbol,
    pm.category,
    pm.tvl_usd,
    pm.change_1d_pct,
    pm.change_7d_pct,
    pm.avg_apy,
    pm.total_pool_tvl,
    pm.pool_count,
    t.current_price_usd      as governance_token_price_usd,
    t.price_change_pct_24h   as governance_token_change_24h,
    t.market_cap_usd         as governance_token_market_cap_usd
from protocol_metrics pm
left join token_by_symbol t on upper(pm.symbol) = t.symbol_u
where pm.symbol is not null and pm.symbol <> ''
