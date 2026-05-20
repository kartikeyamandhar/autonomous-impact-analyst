-- Fails if any ERC-20 transfer amount is negative.
select *
from {{ ref('stg_etherscan__token_transfers') }}
where token_amount < 0
