with source as (
    select * from {{ source('etherscan', 'etherscan_eth_transactions') }}
),

tracked as (
    select * from source
    where hash is not null
    -- The same tx can appear in two tracked addresses' txlists; keep one row.
    qualify row_number() over (partition by hash order by try_cast(blockNumber as bigint) desc) = 1
)

select
    hash                                              as tx_hash,
    try_cast(blockNumber as bigint)                   as block_number,
    timestamp_seconds(try_cast(timeStamp as bigint))  as tx_timestamp,
    lower(`from`)                                     as from_address,
    lower(`to`)                                       as to_address,
    cast(try_cast(value as decimal(38, 0)) / 1e18 as decimal(38, 18)) as value_eth,
    try_cast(gasUsed as bigint)                       as gas_used,
    cast(try_cast(gasPrice as decimal(38, 0)) / 1e9 as decimal(38, 9)) as gas_price_gwei,
    isError = '1'                                     as is_error,
    case
        when lower(`from`) in (
            '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8',
            '0xda9dfa130df4de4673b89022ee50ff26f6ea73cf',
            '0x1b3cb81e51011b549d78bf720b0d924ac763a7c2'
        ) then lower(`from`)
        when lower(`to`) in (
            '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8',
            '0xda9dfa130df4de4673b89022ee50ff26f6ea73cf',
            '0x1b3cb81e51011b549d78bf720b0d924ac763a7c2'
        ) then lower(`to`)
    end                                               as tracked_address
from tracked
