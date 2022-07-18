SELECT
        dexs.block_time,
        project,
        version,
        category,
        46 as protocol_id,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        NULL as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
        -- dYdX Solo Margin v2
        SELECT
            block_timestamp AS block_time,
            'dYdX' AS project,
            'Solo Margin v2' AS version,
            'DEX' AS category,
            takerAccountOwner AS trader_a,
            makerAccountOwner AS trader_b,
            abs(cast(takerOutputUpdate.deltaWei.value as float64))/2 AS token_a_amount_raw, --takerOutputNumber
            abs(cast(takerInputUpdate.deltaWei.value as float64))/2 AS token_b_amount_raw, --takerInputNumber
            NULL AS usd_amount,
            CASE
                WHEN outputMarket = '0' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'                 WHEN outputMarket = '1' THEN '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'                 WHEN outputMarket = '2' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'                 WHEN outputMarket = '3' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'             END AS token_a_address,
            CASE
                WHEN inputMarket = '0' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'                 WHEN inputMarket = '1' THEN '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'                 WHEN inputMarket = '2' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'                 WHEN inputMarket = '3' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'             END AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM blockchain-etl.ethereum_dydx.SoloMargin_event_LogTrade

        UNION ALL

        -- dYdX Perpetual
        SELECT
            block_timestamp AS block_time,
            'dYdX' AS project,
            CASE
                WHEN contract_address = '0x1c50c582c7066049C560Bca20416b1d9E0dfb003' THEN 'PLINK-USDC Perpetual'
                WHEN contract_address = '0x07aBe965500A49370D331eCD613c7AC47dD6e547' THEN 'PBTC-USDC Perpetual'
                WHEN contract_address = '0x09403FD14510F8196F7879eF514827CD76960B5d' THEN 'WETH-PUSD Perpetual'
            END AS version,
            'DEX' AS category,
            maker AS trader_a,
            taker AS trader_b,
            cast(positionAmount as float64) AS token_a_amount_raw,
            cast(marginAmount as float64) AS token_b_amount_raw,
            CASE
                WHEN contract_address = '0x09403FD14510F8196F7879eF514827CD76960B5d' THEN cast(positionAmount as float64)/1e6
                ELSE NULL             END AS usd_amount,
            CASE
                WHEN contract_address = '0x1c50c582c7066049C560Bca20416b1d9E0dfb003' THEN '0x514910771af9ca656af840dff83e8264ecf986ca'                 WHEN contract_address = '0x07aBe965500A49370D331eCD613c7AC47dD6e547' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'                 WHEN contract_address = '0x09403FD14510F8196F7879eF514827CD76960B5d' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'             END AS token_a_address,
            CASE
                WHEN contract_address = '0x1c50c582c7066049C560Bca20416b1d9E0dfb003' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'                 WHEN contract_address = '0x07aBe965500A49370D331eCD613c7AC47dD6e547' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'                 WHEN contract_address = '0x09403FD14510F8196F7879eF514827CD76960B5d' THEN NULL             END AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM blockchain-etl.ethereum_dydx.PerpetualV1_event_LogTrade
        WHERE isBuy = 'true'
    ) dexs

    