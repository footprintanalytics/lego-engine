SELECT  ctoken.contract_address         AS pool_id,
        10                              AS protocol_id,
        "Compound"                      AS project,
        "Ethereum"                      AS chain,
        "lending"                       AS business_type,
        ctoken.contract_address         AS deposit_contract,
        ctoken.contract_address         AS withdraw_contract,
        NULL                            AS lp_token,
        ctoken.underlying_token_address AS stake_token,
        ctoken.underlying_token_address AS stake_underlying_token,
        NULL                            AS reward_token,
        ctoken.symbol                   AS name,
        NULL                            AS description
FROM `xed-project-237404.footprint_etl.compound_view_ctokens` ctoken
-- 由于 upload_pools.py 的 sql 上传需要 match_date_filter
-- 所以暂时用以上查询结果然后以 csv 的方式上传
