with source as (
    select * from {{ source('coingecko', 'coingecko_coins_detail') }}
)

select
    id                                                       as coin_id,
    symbol                                                   as symbol,
    name                                                     as coin_name,
    get_json_object(description, '$.en')                     as description,
    categories                                               as categories,
    get_json_object(links, '$.homepage[0]')                  as homepage_url,
    get_json_object(links, '$.repos_url.github[0]')          as github_url,
    try_cast(genesis_date as date)                           as genesis_date,
    try_cast(get_json_object(developer_data, '$.forks') as int)   as developer_forks,
    try_cast(get_json_object(developer_data, '$.stars') as int)   as developer_stars,
    try_cast(get_json_object(community_data, '$.twitter_followers') as int) as twitter_followers
from source
where id is not null
