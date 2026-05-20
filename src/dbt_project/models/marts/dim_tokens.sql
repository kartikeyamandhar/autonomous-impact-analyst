select
    coin_id,
    symbol,
    coin_name,
    description,
    categories,
    homepage_url,
    github_url,
    genesis_date,
    developer_forks,
    developer_stars
from {{ ref('stg_coingecko__coins_detail') }}
