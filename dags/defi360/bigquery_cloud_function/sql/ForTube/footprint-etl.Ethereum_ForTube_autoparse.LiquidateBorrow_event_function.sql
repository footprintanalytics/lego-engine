CREATE OR REPLACE FUNCTION
    `footprint-etl.Ethereum_ForTube_autoparse.LiquidateBorrow_event_function`(data STRING)
    RETURNS STRUCT<
    `user_address` STRING,
    `token_address` STRING,
    `cheque_token_address` STRING,
    `debt_written_off` STRING,
    `interest_accrued` STRING,
    `debtor_address` STRING,
    `collateral_purchased` STRING,
    `collateral_cheque_token_address` STRING,
    `debtor_balance` STRING,
    `debt_remaining` STRING,
    `cheque_token_value` STRING,
    `loan_interest_rate` STRING,
    `account_balance` STRING,
    `global_token_reserved` STRING,
    `temp1` STRING,
    `temp2` STRING,
     error STRING>
    LANGUAGE js AS """
    var parsedEvent = {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "address", "name": "user_address", "type": "address"},
            {"indexed": false, "internalType": "address", "name": "token_address", "type": "address"},
            {"indexed": false, "internalType": "address", "name": "cheque_token_address", "type": "address"},
            {"indexed": false, "internalType": "uint256", "name": "debt_written_off", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "interest_accrued", "type": "uint256"},
            {"indexed": false, "internalType": "address", "name": "debtor_address", "type": "address"},
            {"indexed": false, "internalType": "uint256", "name": "collateral_purchased", "type": "uint256"},
            {"indexed": false, "internalType": "address", "name": "collateral_cheque_token_address", "type": "address"},
            {"indexed": false, "internalType": "uint256", "name": "debtor_balance", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "debt_remaining", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "cheque_token_value", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "loan_interest_rate", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "account_balance", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "global_token_reserved", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "temp1", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "temp2", "type": "uint256"}],
              "name": "LiquidateBorrow", "type": "event"}
    return abi.decodeEvent(parsedEvent, data);

"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );