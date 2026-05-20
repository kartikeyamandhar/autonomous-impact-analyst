select
    protocol_id,
    protocol_name,
    symbol,
    category,
    chains,
    tvl_usd,
    change_1d_pct,
    change_7d_pct,
    audit_count,
    url,
    slug
from {{ ref('stg_defi_llama__protocols') }}
