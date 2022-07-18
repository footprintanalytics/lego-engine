with view_tokens as (
    SELECT
                ROW_NUMBER() OVER (ORDER BY tokens.block_number, tokens.transaction_index) as token_id,
                tokens.token,
                tokens.block_timestamp as add_date
    FROM `footprint-etl.ethereum_gnosis_protocol.BatchExchange_call_addToken` tokens where status = 1 
),
trades AS (
SELECT
	trades_aux.*
FROM (
	SELECT
		-- id
	    trades.owner AS trader_hex,
	    trades.orderId AS order_id,
	    -- Event index
	    trades.log_index AS evt_index_trades,
	    solution.log_index AS evt_index_solution,
	    -- dates & block info
	    solution.block_number as evt_block_number,
	    solution.block_timestamp AS block_time,
	    -- sell token
	    trades.sellToken AS sell_token_id,
	    sell_token.token AS sell_token,
	    -- sell amounts
	    trades.executedSellAmount AS sell_amount_atoms,
	    -- buy token
	    trades.buyToken AS buy_token_id,
	    buy_token.token AS buy_token,
	    -- buy amounts
	    trades.executedBuyAmount AS buy_amount_atoms,
	    -- Tx and block info
	    trades.block_number AS block_number,
	    trades.transaction_hash AS tx_hash
	FROM footprint-etl.ethereum_gnosis_protocol.BatchExchange_event_Trade trades
	JOIN footprint-etl.ethereum_gnosis_protocol.BatchExchange_event_SolutionSubmission solution
	    ON solution.transaction_hash=trades.transaction_hash
	JOIN view_tokens buy_token
	    ON cast(trades.buyToken as string) = cast(buy_token.token_id as string)
	JOIN view_tokens sell_token
	    ON cast(trades.sellToken as string) = cast(sell_token.token_id as string)
) AS trades_aux)

SELECT
        dexs.block_time,
        project,
        version,
        category,
        138 as protocol_id,
        token_a_amount_raw,
        token_b_amount_raw,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        log_index,
        NULL AS trace_address
    FROM (
        -- V1
        SELECT
            block_time,
            'Gnosis Protocol' AS project,
            '1' AS version,
            'DEX' AS category,
            trader_hex AS trader_a,
            NULL AS trader_b,
            cast (sell_amount_atoms as float64) / 2 AS token_a_amount_raw,
            cast (buy_amount_atoms as float64) / 2 AS token_b_amount_raw,
            NULL  AS usd_amount,
            sell_token AS token_a_address,
            buy_token AS token_b_address,
            '0x6f400810b62df8e13fded51be75ff5393eaa841f' AS exchange_contract_address,
            tx_hash,
            NULL AS trace_address,
            evt_index_trades as log_index
        FROM trades

        UNION ALL

        SELECT
            t.block_timestamp AS block_time,
            'Gnosis Protocol' AS project,
            '2' AS version,
            'Aggregator' AS category,
            t.owner AS trader_a,
            NULL AS trader_b,
            cast (t.buyAmount as float64) AS token_a_amount_raw,
            cast (t.sellAmount as float64) AS token_b_amount_raw,
            NULL AS usd_amount,
            t.buyToken token_a_address,
            t.sellToken token_b_address,
            t.contract_address exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index
        FROM footprint-etl.ethereum_gnosis_protocol.GPv2Settlement_event_Trade t
    ) dexs