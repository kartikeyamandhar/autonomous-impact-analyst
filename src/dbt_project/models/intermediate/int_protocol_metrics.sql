with protocols as (
    select * from {{ ref('stg_defi_llama__protocols') }}
),

pools_agg as (
    select
        project_name,
        avg(apy)        as avg_apy,
        sum(tvl_usd)    as total_pool_tvl,
        count(*)        as pool_count
    from {{ ref('stg_defi_llama__yields_pools') }}
    where project_name is not null
    group by project_name
)

select
    p.protocol_id,
    p.protocol_name,
    p.symbol,
    p.category,
    p.tvl_usd,
    p.change_1d_pct,
    p.change_7d_pct,
    pa.avg_apy,
    pa.total_pool_tvl,
    coalesce(pa.pool_count, 0) as pool_count
from protocols p
left join pools_agg pa on lower(p.slug) = lower(pa.project_name)
