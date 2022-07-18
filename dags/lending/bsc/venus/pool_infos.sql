SELECT pool_id,
       protocol_id,
       project,
       chain,
       business_type,
       deposit_contract,
       withdraw_contract,
       '' as lp_token,
       stake_token,
       CAST(NULL AS ARRAY<string>) AS stake_underlying_token,
       reward_token,
       CONCAT('v', tokena.symbol) AS name,
       '' AS description
FROM (
         SELECT
                 contract_address                 AS pool_id,
                 118                                   AS protocol_id,
                "Venus"                              AS project,
                "Binance"                           AS chain,
                "lending"                                AS business_type,
                contract_address                     AS deposit_contract,
                contract_address                     AS withdraw_contract,
                contract_address                     AS lp_token,
                ARRAY_AGG(DISTINCT token_b_address ) AS stake_token,
             CAST(NULL AS ARRAY<string>) AS reward_token,
             MAX (token_b_address) token0
         FROM (
             select * from (
                 select distinct vs.contract_address, vs.block_time, vv.token_address as token_b_address from `footprint-etl.bsc_lending_venus.bsc_venus_lending_supply_all` vs
                left join `footprint-etl.bsc_venus.venus_vtokens` vv
                on vs.contract_address = vv.pool_address
             )
             WHERE
                DATE(block_time) {match_date_filter} )
         GROUP BY contract_address) t
         LEFT JOIN
     `xed-project-237404.footprint_etl.bsc_erc20_tokens` tokena
     ON
         LOWER(tokena.contract_address) = LOWER(t.token0)
