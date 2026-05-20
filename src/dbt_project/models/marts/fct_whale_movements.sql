with whale_activity as (
    select * from {{ ref('int_whale_activity') }}
),

token_meta as (
    select
        contract_address,
        coin_id,
        symbol,
        coin_name
    from {{ ref('int_token_profiles') }}
    where contract_address is not null
    qualify row_number() over (partition by contract_address order by market_cap_usd desc nulls last) = 1
)

select
    wa.address,
    wa.tx_count,
    wa.total_value,
    wa.unique_counterparties,
    tm.coin_id      as token_coin_id,
    tm.symbol       as token_symbol,
    tm.coin_name    as token_name
from whale_activity wa
left join token_meta tm on wa.address = tm.contract_address
