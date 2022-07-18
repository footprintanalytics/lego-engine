CREATE OR REPLACE FUNCTION
    `footprint-etl.Ethereum_ForTube_autoparse.ReserveWithdrawal_event_function`(data STRING)
    RETURNS STRUCT<
    `token_address` STRING,
    `reserve_withdrawed` STRING,
    `cheque_token_value` STRING,
    `loan_interest_rate` STRING,
    `global_token_reserved` STRING,
     error STRING>
    LANGUAGE js AS """
    var parsedEvent = {
		"anonymous": false,
		"inputs": [
			{
				"indexed": false,
				"internalType": "address",
				"name": "token_address",
				"type": "address"
			},
			{
				"indexed": false,
				"internalType": "uint256",
				"name": "reserve_withdrawed",
				"type": "uint256"
			},
			{
				"indexed": false,
				"internalType": "uint256",
				"name": "cheque_token_value",
				"type": "uint256"
			},
			{
				"indexed": false,
				"internalType": "uint256",
				"name": "loan_interest_rate",
				"type": "uint256"
			},
			{
				"indexed": false,
				"internalType": "uint256",
				"name": "global_token_reserved",
				"type": "uint256"
			}
		],
		"name": "ReserveWithdrawal",
		"type": "event"
	}
    return abi.decodeEvent(parsedEvent, data);

"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );