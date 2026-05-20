with eth_transfers as (
    select
        from_address    as address,
        to_address      as counterparty,
        value_eth       as transfer_value
    from {{ ref('stg_etherscan__transactions') }}
),

token_transfers as (
    select
        from_address    as address,
        to_address      as counterparty,
        token_amount    as transfer_value
    from {{ ref('stg_etherscan__token_transfers') }}
),

unified as (
    select * from eth_transfers
    union all
    select * from token_transfers
)

select
    address,
    count(*)                        as tx_count,
    sum(transfer_value)             as total_value,
    count(distinct counterparty)    as unique_counterparties
from unified
where address is not null
group by address
