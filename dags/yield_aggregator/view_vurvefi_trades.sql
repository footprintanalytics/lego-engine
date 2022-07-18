--SELECT
--    block_timestamp,
--    'Curve' AS project,
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
--        WHEN CAST(bought_id AS INT64) = 3 THEN '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
--        WHEN CAST(sold_id AS INT64) = 3 THEN '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.susd_v2_evt_TokenExchange`--FROM `blockchain-etl.ethereum_curve.sUSDSwap_event_TokenExchange`
--
--UNION ALL
--
--SELECT
--    block_timestamp,
--    'Curve' AS project,
--
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
--        WHEN CAST(bought_id AS INT64) = 3 THEN '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
--        WHEN CAST(sold_id AS INT64) = 3 THEN '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.susd_v2_evt_TokenExchangeUnderlying`
--UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0xf61718057901f84c4eec4339ef8f0d86d2b45600'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0xf61718057901f84c4eec4339ef8f0d86d2b45600'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,
    log_index
FROM `blockchain-etl.ethereum_curve.sUSDSwap_event_TokenExchange`

UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0xf61718057901f84c4eec4339ef8f0d86d2b45600'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0xf61718057901f84c4eec4339ef8f0d86d2b45600'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.sUSDSwap_event_TokenExchangeUnderlying`

UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.CompoundSwap_event_TokenExchange`

UNION ALL

--SELECT
--    block_timestamp,
--    'Curve' AS project,
--
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.compound_v2Swap_event_TokenExchange`
--UNION ALL

--SELECT
--    block_timestamp,
--    'Curve' AS project,
--
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.compound_v2Swap_event_TokenExchangeUnderlying`
--UNION ALL

--SELECT
--    block_timestamp,
--    'Curve' AS project,
--
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.compound_v3Swap_event_TokenExchange`
--UNION ALL

--SELECT
--    block_timestamp,
--    'Curve' AS project,
--
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.compound_v3Swap_event_TokenExchangeUnderlying`
--UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.USDTSwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.USDTSwap_event_TokenExchangeUnderlying`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x16de59092dae5ccf4a1e6439d611fd0653f0bd01'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xd6ad7a6750a7593e092a9b218d66c0a814a3436e'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0x83f798e925bcd4017eb265844fddabb448f1707d'
        WHEN CAST(bought_id AS INT64) = 3 THEN '0x73a052500105205d34daf004eab301916da8190f'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x16de59092dae5ccf4a1e6439d611fd0653f0bd01'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xd6ad7a6750a7593e092a9b218d66c0a814a3436e'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0x83f798e925bcd4017eb265844fddabb448f1707d'
        WHEN CAST(sold_id AS INT64) = 3 THEN '0x73a052500105205d34daf004eab301916da8190f'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.ySwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN CAST(bought_id AS INT64) = 3 THEN '0x0000000000085d4780b73119b644ae5ecd22b376'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN CAST(sold_id AS INT64) = 3 THEN '0x0000000000085d4780b73119b644ae5ecd22b376'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.ySwap_event_TokenExchangeUnderlying`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0xc2cb1040220768554cf699b0d863a3cd4324ce32'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0x26ea744e5b887e5205727f55dfbe8685e3b21951'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xe6354ed5bc4b393a5aad09f21c46e101e692d447'
        WHEN CAST(bought_id AS INT64) = 3 THEN '0x04bc0ab673d88ae9dbc9da2380cb6b79c4bca9ae'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0xc2cb1040220768554cf699b0d863a3cd4324ce32'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0x26ea744e5b887e5205727f55dfbe8685e3b21951'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xe6354ed5bc4b393a5aad09f21c46e101e692d447'
        WHEN CAST(sold_id AS INT64) = 3 THEN '0x04bc0ab673d88ae9dbc9da2380cb6b79c4bca9ae'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.BUSDSwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN CAST(bought_id AS INT64) = 3 THEN '0x4fabb145d64652a948d72533023f6e7a623c7c53'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN CAST(sold_id AS INT64) = 3 THEN '0x4fabb145d64652a948d72533023f6e7a623c7c53'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.BUSDSwap_event_TokenExchangeUnderlying`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x99d1fa417f94dcd62bfe781a1213c092a47041bc'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0x9777d7e2b60bb01759d0e2f8be2095df444cb07e'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0x1be5d71f2da660bfdee8012ddc58d024448a0a59'
        WHEN CAST(bought_id AS INT64) = 3 THEN '0x8e870d67f660d95d5be530380d0ec0bd388289e1'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x99d1fa417f94dcd62bfe781a1213c092a47041bc'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0x9777d7e2b60bb01759d0e2f8be2095df444cb07e'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0x1be5d71f2da660bfdee8012ddc58d024448a0a59'
        WHEN CAST(sold_id AS INT64) = 3 THEN '0x8e870d67f660d95d5be530380d0ec0bd388289e1'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.PAXSwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN CAST(bought_id AS INT64) = 3 THEN '0x8e870d67f660d95d5be530380d0ec0bd388289e1'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN CAST(sold_id AS INT64) = 3 THEN '0x8e870d67f660d95d5be530380d0ec0bd388289e1'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.PAXSwap_event_TokenExchangeUnderlying`
UNION ALL

--SELECT
--    block_timestamp,
--    'Curve' AS project,
--
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        --change address back to renBTC's, right now Dune only tracks WBTC price
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
--    END as token_a_address,
--    CASE
--        --change address back to renBTC's, right now Dune only tracks WBTC price
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.renbtcSwap_event_TokenExchange`
--UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        --change address back to renBTC's, right now Dune only tracks WBTC price
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'
    END as token_a_address,
    CASE
        --change address back to renBTC's, right now Dune only tracks WBTC price
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.sBTCSwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        --change address back to renBTC's, right now Dune only tracks WBTC price
        WHEN CAST(bought_id AS INT64) = 0 THEN '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
    END as token_a_address,
    CASE
        --change address back to renBTC's, right now Dune only tracks WBTC price
        WHEN CAST(sold_id AS INT64) = 0 THEN '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.hBTCSwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(bought_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN CAST(sold_id AS INT64) = 2 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.3poolSwap_event_TokenExchange`
UNION ALL

SELECT
    block_timestamp,
    'Curve' AS project,

    buyer,

    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN CAST(bought_id AS INT64) = 0 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        WHEN CAST(bought_id AS INT64) = 1 THEN '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'
    END as token_a_address,
    CASE
        WHEN CAST(sold_id AS INT64) = 0 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        WHEN CAST(sold_id AS INT64) = 1 THEN '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash,

    log_index
FROM `blockchain-etl.ethereum_curve.stETHSwap_event_TokenExchange`
--UNION ALL
--
--SELECT
--    block_timestamp,
--    'Curve' AS project,
--    '2' AS version,
--    buyer,
--
--    tokens_bought AS token_a_amount_raw,
--    tokens_sold AS token_b_amount_raw,
--    CASE
--        WHEN CAST(bought_id AS INT64) = 0 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
--        WHEN CAST(bought_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
--        WHEN CAST(bought_id AS INT64) = 2 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
--    END as token_a_address,
--    CASE
--        WHEN CAST(sold_id AS INT64) = 0 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
--        WHEN CAST(sold_id AS INT64) = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
--        WHEN CAST(sold_id AS INT64) = 2 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
--    END as token_b_address,
--    contract_address AS exchange_contract_address,
--    transaction_hash,
--
--    log_index
--FROM `blockchain-etl.ethereum_curve.tricryptoSwap_event_TokenExchange`
