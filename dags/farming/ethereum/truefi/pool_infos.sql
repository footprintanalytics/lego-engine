-- pool_infos
SELECT pool_id,
       71                          AS protocol_id,
       'TrueFi'                    AS project,
       "Ethereum"                  AS chain,
       "farming"                   AS business_type,
       deposit_contract,
       withdraw_contract,
       CAST(lp_token as String),
       stake_token,
       CAST(NULL AS ARRAY<string>) AS stake_underlying_token,
       reward_token,
       ''                          AS name,
       ''                          AS description
FROM (
-- 稳定币池，投四种tf稳定币 得 TRU
         SELECT '0xec6c3fd795d6e6f202825ddb56e01b3c128b0b10'                            as pool_id,
                '0xec6c3fd795d6e6f202825ddb56e01b3c128b0b10'                            as deposit_contract,
                '0xec6c3fd795d6e6f202825ddb56e01b3c128b0b10'                            as withdraw_contract,
                null                                                                    as lp_token,
                ARRAY_AGG(tokens)                                                       AS stake_token,
                CAST(['0x4c19596f5aaff459fa38b0f7ed92f11ae6543784'] AS array<STRING > ) AS reward_token,
                null                                                                    as token0,
                null                                                                    as token1
         from (SELECT distinct (token_address) as tokens
               FROM `footprint-etl.ethereum_farming_truefi.truefi_farming_supply_all`
               where token_address != '0x4c19596f5aaff459fa38b0f7ed92f11ae6543784')
         UNION ALL
-- TRU Stake 池，固定的pool，投 TRU 得 TRU 和 tfUSDC 或 tfTUSD
         SELECT '0x23696914Ca9737466D8553a2d619948f548Ee424'                                                                                                                      as pool_id,
                '0x23696914Ca9737466D8553a2d619948f548Ee424'                                                                                                                      as deposit_contract,
                '0x23696914Ca9737466D8553a2d619948f548Ee424'                                                                                                                      as withdraw_contract,
                null                                                                                                                                                              as lp_token,
                CAST(['0x4c19596f5aaff459fa38b0f7ed92f11ae6543784'] AS array<STRING > )                                                                                           AS stake_token,
                CAST(['0x4c19596f5aaff459fa38b0f7ed92f11ae6543784','0xa1e72267084192db7387c8cc1328fade470e4149','0xa991356d261fbaf194463af6df8f0464f8f1c742'] AS array<STRING > ) AS reward_token,
                null                                                                                                                                                              as token0,
                null                                                                                                                                                              as token1,
     )
where '2021-12-15' {match_date_filter}