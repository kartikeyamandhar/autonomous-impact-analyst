with source as (
    select * from {{ source('defi_llama', 'defi_llama_protocols') }}
)

select
    id                                       as protocol_id,
    name                                     as protocol_name,
    symbol                                   as symbol,
    category                                 as category,
    chains                                   as chains,
    try_cast(tvl as decimal(38, 2))          as tvl_usd,
    try_cast(change_1d as decimal(18, 8))    as change_1d_pct,
    try_cast(change_7d as decimal(18, 8))    as change_7d_pct,
    try_cast(audits as int)                  as audit_count,
    url                                      as url,
    slug                                     as slug
from source
where id is not null
