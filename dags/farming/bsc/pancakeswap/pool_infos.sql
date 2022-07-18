
-- pool_infos
SELECT pool_id,
       100                          AS protocol_id,
       "Pancakeswap"                AS project,
       "Binance"                    AS chain,
       "farming"                    AS business_type,
       deposit_contract,
       withdraw_contract,
       lp_token,
       stake_token,
       CAST(NULL AS ARRAY<string>)  AS stake_underlying_token,
       reward_token,
       CONCAT(
               IF
                   (tokena.symbol IS NULL,
                    token0,
                    tokena.symbol), '/',
               IF
                   (tokenb.symbol IS NULL,
                    token1,
                    tokenb.symbol)) AS name,
       ''                           AS description
FROM (
--     LP 的
         SELECT pair                                                                    AS pool_id,
                pair                                                                    AS deposit_contract,
                pair                                                                    AS withdraw_contract,
                pair                                                                    AS lp_token,
                ARRAY_AGG(DISTINCT token_b_address )                                    AS stake_token,
                CAST(['0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'] AS array<STRING > ) AS reward_token,
                MAX(token0)                                                                token0,
                MAX(token1)                                                                token1,
         FROM (
                  select *
                  from (
                           SELECT block_timestamp,
                                  pair,
                                  token0,
                                  token1,
                                  token0 AS token_b_address
                           FROM `footprint-etl.bsc_pancakeswap.UniswapV2Pair_event_PairCreated`
                           union all
                           SELECT block_timestamp,
                                  pair,
                                  token0,
                                  token1,
                                  token1 AS token_b_address
                           FROM `footprint-etl.bsc_pancakeswap.UniswapV2Pair_event_PairCreated`
                       )
                  WHERE
                      DATE (block_timestamp) {match_date_filter})
GROUP BY pair
UNION ALL
--  普通的单币池子 todo 还没有没有对应的reward token ,可能需要连reward表拿到reward token,暂时不获取
SELECT smartChef                                                               as pool_id,
       smartChef                                                               as deposit_contract,
       smartChef                                                               as withdraw_contract,
       NULL                                                                    as lp_token,
       CAST(['0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'] AS array<STRING > ) AS stake_token,
       CAST(NULL AS array<STRING > )                                           AS reward_token,
       null                                                                    as token0,
       null                                                                    as token1,
FROM `footprint-etl.bsc_pancakeswap.SmartChefFactory_event_NewSmartChefContract`
UNION ALL
-- AutoCake 自动池，投cake 得cake，有自己的合约 0xa80240Eb5d7E05d3F250cF000eEc0891d00b51CC
SELECT '0xa80240Eb5d7E05d3F250cF000eEc0891d00b51CC'                            as pool_id,
       '0xa80240Eb5d7E05d3F250cF000eEc0891d00b51CC'                            as deposit_contract,
       '0xa80240Eb5d7E05d3F250cF000eEc0891d00b51CC'                            as withdraw_contract,
       null                                                                    as lp_token,
       CAST(['0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'] AS array<STRING > ) AS stake_token,
       CAST(['0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'] AS array<STRING > ) AS reward_token,
       null                                                                    as token0,
       null                                                                    as token1
UNION ALL
-- Cake 手动池，投cake 得cake，没有直接的合约，直接调用MasterChef主合约，所以用cake的地址作为pool id
SELECT '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'                            as pool_id,
       '0x73feaa1eE314F8c655E354234017bE2193C9E24E'                            as deposit_contract,
       '0x73feaa1eE314F8c655E354234017bE2193C9E24E'                            as withdraw_contract,
       null                                                                    as lp_token,
       CAST(['0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'] AS array<STRING > ) AS stake_token,
       CAST(['0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'] AS array<STRING > ) AS reward_token,
       null                                                                    as token0,
       null                                                                    as token1,
    ) t
    LEFT JOIN
    `xed-project-237404.footprint_etl.bsc_erc20_tokens` tokena
ON
    LOWER (tokena.contract_address) = LOWER (t.token0)
    LEFT JOIN
    `xed-project-237404.footprint_etl.bsc_erc20_tokens` tokenb
    ON
    LOWER (tokenb.contract_address) = LOWER (t.token1)