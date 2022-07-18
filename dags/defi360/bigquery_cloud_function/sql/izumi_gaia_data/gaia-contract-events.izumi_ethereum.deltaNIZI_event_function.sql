CREATE OR REPLACE FUNCTION
    `gaia-contract-events.izumi_ethereum.deltaNIZI_event_function`(data STRING) RETURNS STRUCT<tokenId STRING, deltaNIZI STRING, error STRING> LANGUAGE js
AS """
var abi = {"inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"internalType": "uint256", "name": "deltaNIZI", "type": "uint256"}], "name": "depositIZI", "outputs": [], "stateMutability": "nonpayable", "type": "function"};
    var interface_instance = new ethers.utils.Interface([abi]);

    var result = {};
    try {
        var parsedTransaction = interface_instance.parseTransaction({data: data});
        var parsedArgs = parsedTransaction.args;

        if (parsedArgs && parsedArgs.length >= abi.inputs.length) {
            for (var i = 0; i < abi.inputs.length; i++) {
                var paramName = abi.inputs[i].name;
                var paramValue = parsedArgs[i];
                if (abi.inputs[i].type === 'address' && typeof paramValue === 'string') {
                    // For consistency all addresses are lowercase.
                    paramValue = paramValue.toLowerCase();
                }
                result[paramName] = paramValue;
            }
        } else {
            result['error'] = 'Parsed transaction args is empty or has too few values.';
        }
    } catch (e) {
        result['error'] = e.message;
    }

    return result;
"""

OPTIONS
    (library=["gs://blockchain-etl-bigquery/ethers.js","https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js"])