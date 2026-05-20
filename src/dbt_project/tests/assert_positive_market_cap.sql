-- Fails if any token has a negative market cap.
select *
from {{ ref('fct_daily_token_metrics') }}
where market_cap_usd < 0
