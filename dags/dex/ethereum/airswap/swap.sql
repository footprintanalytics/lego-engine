SELECT
        dexs.block_time,
        project,
        protocol_id,
        version,
        category,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        NULL AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id
    FROM (
        SELECT
            block_timestamp as block_time,
            'Airswap' as project,
            '2' as version,
            'DEX' as category,
            403 as protocol_id,
            senderWallet as trader_a, --define taker as trader a
            signerWallet as trader_b, --define maker as trader b
            senderAmount as token_a_amount_raw,
            signerAmount as token_b_amount_raw,
            senderToken as token_a_address,
            signerToken as token_b_address,
            contract_address as exchange_contract_address,
            transaction_hash as tx_hash,
            NULL as trace_address,
            NULL as usd_amount,
            log_index AS evt_index
        FROM blockchain-etl.ethereum_airswap.Light_event_Swap

        UNION ALL

        SELECT
            block_timestamp as block_time,
            'Airswap' as project,
            '2' as version,
            'DEX' as category,
            403 as protocol_id,
            senderWallet as trader_a, --define taker as trader a
            signerWallet as trader_b, --define maker as trader b
            senderAmount as token_a_amount_raw,
            signerAmount as token_b_amount_raw,
            senderToken as token_a_address,
            signerToken as token_b_address,
            contract_address as exchange_contract_address,
            transaction_hash as tx_hash,
            NULL as trace_address,
            NULL as usd_amount,
            log_index AS evt_index
        FROM blockchain-etl.ethereum_airswap.Swap_event_Swap
    ) dexs
    