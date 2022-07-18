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
       CONCAT('av', tokena.symbol) AS name,
       '' AS description
FROM (
         SELECT aToken          AS pool_id,
                 791                                   AS protocol_id,
                "Aave"                              AS project,
                "Avalanche"                           AS chain,
                "lending"                                AS business_type,
                '0x4f01aed16d97e3ab5ab2b501154dc9bb0f1a5a2c'                     AS deposit_contract,
                '0x4f01aed16d97e3ab5ab2b501154dc9bb0f1a5a2c'                     AS withdraw_contract,
                ''                     AS lp_token,
                ARRAY_AGG(DISTINCT asset ) AS stake_token,
             CAST(NULL AS ARRAY<string>) AS reward_token,
             MAX (asset) token0
         FROM (
             select * from (
                 select block_timestamp, aToken, asset from `footprint-etl.avalanche_aave.LendingPoolConfigurator_event_ReserveInitialized`
             )
             WHERE
                DATE(block_timestamp) {match_date_filter} )
         GROUP BY aToken) t
         LEFT JOIN
     `xed-project-237404.footprint_etl.avalanche_erc20_tokens` tokena
     ON
         LOWER(tokena.contract_address) = LOWER(t.token0)
