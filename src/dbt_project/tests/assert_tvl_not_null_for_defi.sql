-- Fails if any protocol whose category mentions 'defi' has a null TVL.
select *
from {{ ref('dim_protocols') }}
where lower(category) like '%defi%'
  and tvl_usd is null
