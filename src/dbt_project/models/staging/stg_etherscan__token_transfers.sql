with source as (
    select * from {{ source('etherscan', 'etherscan_token_transfers') }}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['hash', '`from`', '`to`', 'contractAddress', 'value']) }} as transfer_id,
        *
    from source
    where hash is not null
    qualify row_number() over (
        partition by {{ dbt_utils.generate_surrogate_key(['hash', '`from`', '`to`', 'contractAddress', 'value']) }}
        order by try_cast(blockNumber as bigint) desc
    ) = 1
)

select
    transfer_id                                       as transfer_id,
    hash                                              as tx_hash,
    try_cast(blockNumber as bigint)                   as block_number,
    timestamp_seconds(try_cast(timeStamp as bigint))  as tx_timestamp,
    lower(`from`)                                     as from_address,
    lower(`to`)                                       as to_address,
    cast(
        try_cast(value as double) / power(10, try_cast(tokenDecimal as int))
        as decimal(38, 18)
    )                                                 as token_amount,
    tokenName                                         as token_name,
    tokenSymbol                                       as token_symbol,
    lower(contractAddress)                            as contract_address
from keyed
