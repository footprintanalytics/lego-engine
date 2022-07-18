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
       IF
           (tokena.symbol IS NULL,
            token0,
            tokena.symbol)  AS name,
       ''                   AS description
FROM (
         SELECT contract_address                  AS pool_id,
                38                                AS protocol_id,
                "Raricapital"                     AS project,
                "Ethereum"                        AS chain,
                "lending"                         AS business_type,
                contract_address                  AS deposit_contract,
                contract_address                  AS withdraw_contract,
                CAST(NULL AS string)              AS lp_token,
                ARRAY_AGG(DISTINCT token_address) AS stake_token,
                CAST(NULL AS ARRAY<string>)       AS reward_token,
                MAX(token_address)                   token0,
         FROM (
                  SELECT *
                  FROM `footprint-etl.ethereum_lending_raricapital.raricapital_lending_supply_all`
                  where token_address != contract_address
                  and DATE (block_time) {match_date_filter})
         GROUP BY contract_address) t
         LEFT JOIN
     `xed-project-237404.footprint_etl.erc20_tokens` tokena
     ON
         LOWER(tokena.contract_address) = LOWER(t.token0)