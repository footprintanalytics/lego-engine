WITH
    day_list AS (
        SELECT
    day
FROM
    UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('2021-10-20 00:00:00'),
    CURRENT_TIMESTAMP(),
    INTERVAL 1 day)) AS day )
SELECT
    DATE(d.day) AS day,
    owner.chain,
    owner.nft_address,
    owner.token_id,
    owner.owner as wallet_address
FROM (
    SELECT
    *,
    LEAD(day, 1, CURRENT_DATE()) OVER (PARTITION BY token_id ORDER BY day) AS next_day
    FROM (
    SELECT
    'Ethereum' AS chain,
    transfer.contract_address AS nft_address,
    transfer.tokenId AS token_id,
    transfer.TO AS owner,
    DATE(transfer.block_timestamp) AS day,
    transfer.transaction_hash
    FROM (
    SELECT
    *
    FROM
    `footprint-etl.ethereum_uniswap.UniswapV3PositionsNFT_event_Transfer` t
    WHERE
    t.TO != '0x0000000000000000000000000000000000000000'
    AND t.tokenId IN (
    SELECT
    DISTINCT tokenId
    FROM
    `gaia-contract-events.izumi_ethereum.parse_deposit_event` ) ) transfer
    LEFT JOIN
    `gaia-contract-events.izumi_ethereum.parse_deposit_event` deposit
    ON
    transfer.tokenId = deposit.tokenId

    UNION ALL

    SELECT
    'Polygon' AS chain,
    transfer.contract_address AS nft_address,
    transfer.tokenId AS token_id,
    transfer.TO AS owner,
    DATE(transfer.block_timestamp) AS day,
    transfer.transaction_hash
    FROM (
    SELECT
    *
    FROM
    `footprint-etl.polygon_uniswap.UniswapV3PositionsNFT_event_Transfer` t
    WHERE
    t.TO != '0x0000000000000000000000000000000000000000'
    AND t.tokenId IN (
    SELECT
    DISTINCT tokenId
    FROM
    `gaia-contract-events.izumi_polygon.parse_deposit_event` ) ) transfer
    LEFT JOIN
    `gaia-contract-events.izumi_polygon.parse_deposit_event` deposit
    ON
    transfer.tokenId = deposit.tokenId

    UNION ALL

    SELECT
    'Arbitrum' AS chain,
    transfer.contract_address AS nft_address,
    transfer.tokenId AS token_id,
    transfer.TO AS owner,
    DATE(transfer.block_timestamp) AS day,
    transfer.transaction_hash
    FROM (
    SELECT
    *
    FROM
    `gaia-contract-events.uniswap_nft_arbitrum.parse_uniswap_v_3_positions_nft_event_transfer_event` t
    WHERE
    t.TO != '0x0000000000000000000000000000000000000000'
    AND t.tokenId IN (
    SELECT
    DISTINCT tokenId
    FROM
    `gaia-contract-events.izumi_arbitrum.parse_deposit_event` ) ) transfer
    LEFT JOIN
    `gaia-contract-events.izumi_arbitrum.parse_deposit_event` deposit
    ON
    transfer.tokenId = deposit.tokenId

    )) owner
    INNER JOIN
    day_list d
ON
    DATE(owner.day) <= DATE(d.day)
    AND DATE(d.day) < DATE(owner.next_day)
WHERE
      DATE(d.day) {run_date}