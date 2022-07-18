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
       CONCAT('am', tokena.symbol) AS name,
       CONCAT('deposit proxy address is ', pool_id ,' ,aToken/ is ', deposit_contract) AS description
FROM (
         SELECT "0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf"           AS pool_id,
                 7                                   AS protocol_id,
                "Aave"                              AS project,
                "Polygon"                           AS chain,
                "lending"                                AS business_type,
                aToken                     AS deposit_contract,
                aToken                     AS withdraw_contract,
                aToken                     AS lp_token,
                ARRAY_AGG(DISTINCT asset ) AS stake_token,
             CAST(NULL AS ARRAY<string>) AS reward_token,
             MAX (asset) token0
         FROM (
             select * from (
                 select block_timestamp, aToken, asset from `footprint-etl.polygon_aave.aaveProxy_event_ReserveInitialized`
             )
             WHERE
                DATE(block_timestamp) {match_date_filter} )
         GROUP BY aToken) t
         LEFT JOIN
     `xed-project-237404.footprint_etl.polygon_erc20_tokens` tokena
     ON
         LOWER(tokena.contract_address) = LOWER(t.token0)
