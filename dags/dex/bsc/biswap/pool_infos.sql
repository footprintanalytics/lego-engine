-- biswap
-- pool_infos
SELECT pool_id,
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
       CONCAT(
               IF
                   (tokena.symbol IS NULL,
                    token0,
                    tokena.symbol), '/',
               IF
                   (tokenb.symbol IS NULL,
                    token1,
                    tokenb.symbol)) AS name,
       '' AS description
FROM (
         SELECT pair                     AS pool_id,
                 309                                   AS protocol_id,
                "Biswap"                              AS project,
                "Binance"                           AS chain,
                "dex"                                AS business_type,
                pair                     AS deposit_contract,
                pair                     AS withdraw_contract,
                pair                     AS lp_token,
                ARRAY_AGG(DISTINCT token_b_address ) AS stake_token,
             CAST(NULL AS ARRAY<string>) AS reward_token,
             MAX (token0) token0,
             MAX (token1) token1,
         FROM (
             select * from (
                 SELECT
             block_timestamp,
             pair,
             token0,
             token1,
             token0 as token_b_address
             FROM `footprint-etl.bsc_biswap.BiswapFactory_event_PairCreated`
            union all
            SELECT
            block_timestamp,
             pair,
             token0,
             token1,
             token1 as token_b_address
             FROM `footprint-etl.bsc_biswap.BiswapFactory_event_PairCreated`
             )
             WHERE
                DATE(block_timestamp) {match_date_filter} )
         GROUP BY pair) t
         LEFT JOIN
     `xed-project-237404.footprint_etl.bsc_erc20_tokens` tokena
     ON
         LOWER(tokena.contract_address) = LOWER(t.token0)
         LEFT JOIN
     `xed-project-237404.footprint_etl.bsc_erc20_tokens` tokenb
     ON
         LOWER(tokenb.contract_address) = LOWER(t.token1)