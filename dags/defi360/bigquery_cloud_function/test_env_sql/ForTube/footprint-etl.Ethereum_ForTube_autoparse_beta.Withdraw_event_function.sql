CREATE FUNCTION `footprint-etl.Ethereum_ForTube_autoparse_beta.Withdraw_event_function`(data STRING)
    RETURNS STRUCT<
    `user_address` STRING, 
    `token_address` STRING, 
    `cheque_token_address` STRING, 
    `amount_withdrawed` STRING, 
    `underlying_withdrawed` STRING, 
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
            {"indexed": false, "internalType": "uint256", "name": "amount_withdrawed", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "underlying_withdrawed", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "cheque_token_value", "type": "uint256"},    
            {"indexed": false, "internalType": "uint256", "name": "loan_interest_rate", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "account_balance", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "global_token_reserved", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "temp1", "type": "uint256"}, 
            {"indexed": false, "internalType": "uint256", "name": "temp2", "type": "uint256"}],
              "name": "Withdraw", "type": "event"}
    return abi.decodeEvent(parsedEvent, data);

"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );