SELECT
    'bzx' AS project,
    '1' AS version,
    103 AS protocol_id,
    'lending' AS type,
    block_number,
    block_timestamp as block_time,
    transaction_hash as tx_hash,
    log_index,
    contract_address,
    user AS operator,
    loanToken AS token_address,
    cast(repayAmount as float64) AS token_amount_raw,
       '' as pool_id
from `footprint-etl.ethereum_bzx.LoanClosings_event_CloseWithDeposit`
            