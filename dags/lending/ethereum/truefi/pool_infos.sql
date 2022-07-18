SELECT
  pool_id,
  protocol_id,
  project,
  chain,
  business_type,
  deposit_contract,
  withdraw_contract,
  lp_token,
  stake_token,
  CAST(NULL AS ARRAY<string>) AS stake_underlying_token,
  reward_token,
IF
  (tokena.symbol IS NULL,
    token0,
    tokena.symbol) AS name,
  '' AS description
FROM (
  SELECT
    contractAddress AS pool_id,
    71 AS protocol_id,
    "TrueFi" AS project,
    "Ethereum" AS chain,
    "lending" AS business_type,
    contractAddress AS deposit_contract,
    contractAddress AS withdraw_contract,
    CAST(NULL AS string) AS lp_token,
    ARRAY_AGG(DISTINCT token_address) AS stake_token,
    CAST(NULL AS ARRAY<string>) AS reward_token,
    MAX(token_address) token0,
  FROM (
    SELECT
      contractAddress,
      token_address
    FROM (
      SELECT
        contractAddress
      FROM
        `footprint-etl.ethereum_truefi.LoanTokenCreator_event_LoanTokenCreated`
      WHERE DATE (block_timestamp) {match_date_filter}
      UNION ALL
      SELECT
        contractAddress
      FROM
        `footprint-etl.ethereum_truefi.LoanTokenCreatorV1_event_LoanTokenCreated`
      WHERE DATE (block_timestamp) {match_date_filter}) pool
    LEFT JOIN
      (select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` WHERE DATE (block_timestamp) {match_date_filter}) tt
    ON
      tt.to_address = pool.contractAddress )
  WHERE
    token_address IS NOT NULL
  GROUP BY
    contractAddress,
    token_address) t
LEFT JOIN
  `xed-project-237404.footprint_etl.erc20_tokens` tokena
ON
  LOWER(tokena.contract_address) = LOWER(t.token0)