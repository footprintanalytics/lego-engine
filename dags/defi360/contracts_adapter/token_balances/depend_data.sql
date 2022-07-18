WITH dodo_deposit AS(
    SELECT
    chain,
    pool_id,
    operator AS holder_address
    FROM `gaia-dao.gaia_dao.Dodo_b3c1966c-9c0f-4c32-8158-e9002bed27a2`
    WHERE business_type = 'dexAddLiquidity'
    GROUP BY 1, 2, 3
    ORDER BY 2
),
pool_info AS(
    SELECT
    chain,
    pool_id,
    deposit_contract_address_list[OFFSET(0)] AS pool_address,
    token_address_list[OFFSET(0)] AS token_a,
    token_address_list[OFFSET(1)] AS token_b
    FROM `gaia-dao.gaia_dao.Dodo_b3c1966c-9c0f-4c32-8158-e9002bed27a2_poolInfo_autoparse`
),
pool_holders AS(
    SELECT
    dd.chain,
    dd.holder_address,
    p.pool_address AS token_address,
    FROM  dodo_deposit dd
    LEFT JOIN pool_info p
    ON dd.pool_id = p.pool_id
),
stake_token_holders AS(
    SELECT
    chain,
    p.pool_address AS holder_address,
    p.token_a AS token_address
    FROM  pool_info p
    UNION ALL
    SELECT
    chain,
    p.pool_address AS holder_address,
    p.token_b AS token_address
    FROM  pool_info p
),
all_token_holders AS (
    SELECT
    *
    FROM  pool_holders
    UNION ALL
    SELECT
    *
    FROM  stake_token_holders
)
SELECT
    chain,
    holder_address,
    token_address
FROM  all_token_holders
GROUP BY 1, 2,3
ORDER BY 3