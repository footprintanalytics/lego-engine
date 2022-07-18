SELECT
     block_timestamp
     ,block_number
     ,transaction_hash
     ,parsed.error AS error
     ,contract_address
     ,log_index
     ,parsed.token_address AS `token_address`
     ,parsed.reserve_withdrawed AS `reserve_withdrawed`
     ,parsed.cheque_token_value AS `cheque_token_value`
     ,parsed.loan_interest_rate AS `loan_interest_rate`
     ,parsed.global_token_reserved AS `global_token_reserved`

FROM (SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,`footprint-etl.Ethereum_ForTube_autoparse.ReserveWithdrawal_event_function`(logs.payload) AS parsed
    ,logs.log_index AS log_index
    ,logs.contract_address AS contract_address
FROM `footprint-etl.Ethereum_ForTube_autoparse.MonitorEvent_event_bytes32Indexed_bytes` AS logs where FuncName='0x526573657276655769746864726177616c000000000000000000000000000000')