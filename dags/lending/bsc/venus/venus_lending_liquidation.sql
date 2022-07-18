SELECT
	'Venus' AS project,
    '1' AS version,
    118 AS protocol_id,
	'liquidation' as type,
	vt.block_number,
	vt.block_timestamp as block_time,
	vt.transaction_hash as tx_hash,
	vt.log_index,
	vt.contract_address,
	vt.borrower,
	vt.vTokenCollateral as token_collateral_address,
	CAST(vt.seizeTokens AS BIGNUMERIC) as token_collateral_amount_raw,
	vt.liquidator,
	vv.token_address as repay_token_address,
	CAST(vt.repayAmount AS BIGNUMERIC) as repay_token_amount_raw,
    vt.contract_address as pool_id
FROM (
    SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, liquidator, borrower, repayAmount, vTokenCollateral, seizeTokens
    from `footprint-etl.bsc_venus.vToken_event_LiquidateBorrow`
--    FROM (
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vSXP_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vXVS_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vUSDC_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vUSDT_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBUSD_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBNB_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBTC_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vETH_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vLTC_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vXRP_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBCH_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vDOT_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vLINK_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vDAI_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vFIL_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBETH_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vADA_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vDOGE_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vMATIC_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vCAKE_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vAAVE_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vTUSD_event_LiquidateBorrow`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vTRX_event_LiquidateBorrow`
--    )
) vt
LEFT JOIN (
    SELECT distinct pool_id as pool_address, stake_token[offset(0)] as token_address FROM `footprint-etl.bsc_lending_venus.pool_infos`
) vv
on lower(vt.contract_address) = lower(vv.pool_address)

WHERE
	Date(block_timestamp) {match_date_filter}
