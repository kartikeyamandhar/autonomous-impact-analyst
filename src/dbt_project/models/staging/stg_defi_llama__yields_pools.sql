with source as (
    select * from {{ source('defi_llama', 'defi_llama_yields_pools') }}
)

select
    pool                                     as pool_id,
    chain                                    as chain,
    project                                  as project_name,
    symbol                                   as symbol,
    try_cast(tvlUsd as decimal(38, 2))       as tvl_usd,
    try_cast(apy as decimal(18, 8))          as apy,
    try_cast(apyBase as decimal(18, 8))      as apy_base,
    try_cast(apyReward as decimal(18, 8))    as apy_reward,
    lower(stablecoin) = 'true'               as is_stablecoin
from source
where pool is not null
