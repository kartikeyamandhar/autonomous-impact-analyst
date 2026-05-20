with markets as (
    select * from {{ ref('stg_coingecko__coins_markets') }}
),

detail as (
    select * from {{ ref('stg_coingecko__coins_detail') }}
),

protocols_by_symbol as (
    select
        upper(symbol)   as symbol_u,
        protocol_id,
        protocol_name,
        tvl_usd,
        category
    from {{ ref('stg_defi_llama__protocols') }}
    where symbol is not null and symbol <> ''
    qualify row_number() over (partition by upper(symbol) order by tvl_usd desc nulls last) = 1
),

token_contracts as (
    select
        upper(token_symbol) as symbol_u,
        contract_address
    from {{ ref('stg_etherscan__token_transfers') }}
    where token_symbol is not null
    qualify row_number() over (partition by upper(token_symbol) order by contract_address) = 1
)

select
    m.coin_id,
    m.symbol,
    m.coin_name,
    m.current_price_usd,
    m.market_cap_usd,
    m.total_volume_usd,
    m.market_cap_rank,
    m.price_change_pct_24h,
    m.all_time_high_usd,
    d.description,
    d.categories,
    d.homepage_url,
    d.github_url,
    d.genesis_date,
    d.developer_forks,
    d.developer_stars,
    p.protocol_id,
    p.protocol_name,
    p.tvl_usd        as protocol_tvl_usd,
    p.category       as protocol_category,
    tc.contract_address
from markets m
left join detail d on m.coin_id = d.coin_id
left join protocols_by_symbol p on upper(m.symbol) = p.symbol_u
left join token_contracts tc on upper(m.symbol) = tc.symbol_u
