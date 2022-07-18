SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,contract_address
     ,log_index
     ,parsed.error AS error
     ,parsed.temp1 as `temp1`
     ,parsed.temp2 as `temp2`
     ,parsed.user_address AS `user_address`
     ,parsed.token_address AS `token_address`
     ,parsed.cheque_token_address AS `cheque_token_address`
     ,parsed.amount_withdrawed AS `amount_repayed`
     ,parsed.underlying_withdrawed AS `underlying_withdrawed`
     ,parsed.cheque_token_value AS `cheque_token_value`
     ,parsed.loan_interest_rate AS `loan_interest_rate`
     ,parsed.account_balance AS `account_balance`
     ,parsed.global_token_reserved AS `global_token_reserved`

FROM (SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,logs.contract_address AS contract_address
    ,`footprint-etl.Ethereum_ForTube_autoparse_beta.Withdraw_event_function`(logs.payload) AS parsed
FROM `footprint-etl.Ethereum_ForTube_autoparse_beta.MonitorEvent_event_bytes32Indexed_bytes` AS logs where FuncName='0x5769746864726177000000000000000000000000000000000000000000000000')
