
WITH
  lp_tokens AS(
  SELECT
    DISTINCT lp_token
  FROM
    `footprint-etl.footprint_pool_infos.pool_infos`
  WHERE
    protocol_id IN (1,3,12,16) ),
  token_transfer AS (
  SELECT
    transaction_hash,
    token_address,
    value,
    from_address,
    to_address
  FROM
    `footprint-blockchain-etl.crypto_ethereum.token_transfers`
  WHERE
    DATE(block_timestamp) >= '2021-04-29' ),
  token_transfer_pair AS (
  SELECT
    token_transfer.*
  FROM
    lp_tokens
  LEFT JOIN
    token_transfer
  ON
    lp_tokens.lp_token = token_transfer.token_address ),
  transactions AS (
  SELECT
    `hash`,
    from_address,
    to_address
  FROM
    `footprint-blockchain-etl.crypto_ethereum.transactions`
  WHERE
    DATE(block_timestamp) >= '2021-04-29'
    AND receipt_status = 1
    AND to_address = '0xba5ebaf3fc1fcca67147050bf80462393814e54b' ),
  trans_flow AS (
  SELECT
    collateral.*,
    transactions.from_address AS operator,
  FROM
    `footprint-etl.ethereum_alpha.HomoraBank_event_PutCollateral` AS collateral
  LEFT JOIN
    transactions
  ON
    transactions.`hash` = collateral.transaction_hash ),
  distinct_transaction_token AS (
  SELECT
    DISTINCT token_transfer_pair.token_address,
    token_transfer_pair.transaction_hash
  FROM
    token_transfer_pair
  LEFT JOIN
    transactions
  ON
    token_transfer_pair.transaction_hash = transactions.`hash` )
SELECT
  'Alpha Finance' AS project,
  '2' AS version,
  61 AS protocol_id,
  trans_flow.block_number,
  trans_flow.block_timestamp,
  trans_flow.transaction_hash,
  trans_flow.log_index,
  trans_flow.contract_address,
  CAST(trans_flow.positionId AS NUMERIC) AS positionId,
  trans_flow.operator,
  distinct_transaction_token.token_address AS asset_address,
  CAST(trans_flow.amount AS BIGNUMERIC) AS asset_amount,
  distinct_transaction_token.token_address as pool_id
FROM
  trans_flow
LEFT JOIN
  distinct_transaction_token
ON
  trans_flow.transaction_hash = distinct_transaction_token.transaction_hash
WHERE
	Date(block_timestamp) {match_date_filter}