 SELECT
        'Venus' AS project,
        '1' AS version,
        118 AS protocol_id,
        'lending' as type,
        block_number,
        block_timestamp as block_time,
        transaction_hash as tx_hash,
        log_index,
        vt.contract_address,
        redeemer as operator,
        vv.token_address as token_address,
        CAST(redeemAmount AS BIGNUMERIC) AS token_amount_raw,
        vt.contract_address as pool_id
from `footprint-etl.bsc_venus.vToken_event_Redeem` vt
--FROM (
--    SELECT *
--        FROM `footprint-etl.bsc_venus.vSXP_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vXVS_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vUSDC_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vUSDT_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBUSD_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBNB_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBTC_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vETH_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vLTC_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vXRP_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBCH_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vDOT_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vLINK_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vDAI_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vFIL_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vBETH_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vADA_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vDOGE_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vMATIC_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vCAKE_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vAAVE_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vTUSD_event_Redeem`
--        UNION ALL
--        SELECT *
--        FROM `footprint-etl.bsc_venus.vTRX_event_Redeem`
--) vt
LEFT JOIN (
    SELECT distinct pool_id as pool_address, stake_token[offset(0)] as token_address FROM `footprint-etl.bsc_lending_venus.pool_infos`
) vv
ON vv.pool_address = vt.contract_address
WHERE DATE(block_timestamp) {match_date_filter}