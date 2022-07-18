with deposit as (
select * from (
    SELECT
        DATE(t.block_timestamp) AS day,
        'Ethereum' AS chain,
        t.block_number,
        t.block_timestamp,
        t.d_tx AS transaction_hash,
        t.log_index,
        t.pool_address,
        CAST(t.tokenId AS FLOAT64) as token_id,
        CAST(m.amount0 AS BIGNUMERIC) as amount0,
        CAST(m.amount1 AS BIGNUMERIC) as amount1
--         (CAST(m.amount0 AS FLOAT64) + CAST(m.amount1 AS FLOAT64)) / 1000000 AS usd_value
    FROM (
        SELECT
        deposit.tokenId,
        deposit.transaction_hash AS d_tx,
        transfer.transaction_hash,
        deposit.block_timestamp,
        deposit.block_number,
        deposit.log_index,
        deposit.contract_address AS pool_address
        FROM
        `footprint-etl.Ethereum_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256` deposit
        LEFT JOIN (
        SELECT
        *
        FROM
        `footprint-etl.ethereum_uniswap.UniswapV3PositionsNFT_event_Transfer` t
        WHERE
        t.from = '0x0000000000000000000000000000000000000000') transfer
        ON
        deposit.tokenId = transfer.tokenId ) t
        LEFT JOIN
        `footprint-etl.ethereum_uniswap.UniswapV3Pool_event_Mint` m
    ON
        t.transaction_hash = m.transaction_hash

    UNION ALL

    SELECT
        DATE(t.block_timestamp) AS day,
        'Polygon' AS chain,
        t.block_number,
        t.block_timestamp,
        t.d_tx AS transaction_hash,
        t.log_index,
        t.pool_address,
        CAST(t.tokenId AS FLOAT64) as token_id,
        CAST(m.amount0 AS BIGNUMERIC) as amount0,
        CAST(m.amount1 AS BIGNUMERIC) as amount1
--         (CAST(m.amount0 AS FLOAT64) + CAST(m.amount1 AS FLOAT64)) / 1000000 AS usd_value
    FROM (
        SELECT
        deposit.tokenId,
        deposit.transaction_hash AS d_tx,
        transfer.transaction_hash,
        deposit.block_timestamp,
        deposit.block_number,
        deposit.log_index,
        deposit.contract_address AS pool_address
        FROM
        `footprint-etl.Polygon_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256` deposit
        LEFT JOIN (
        SELECT
        *
        FROM
        `footprint-etl.polygon_uniswap.UniswapV3PositionsNFT_event_Transfer` t
        WHERE
        t.from = '0x0000000000000000000000000000000000000000') transfer
        ON
        deposit.tokenId = transfer.tokenId ) t
        LEFT JOIN
        `footprint-etl.polygon_uniswap.UniswapV3Pool_event_Mint` m
    ON
        t.transaction_hash = m.transaction_hash
) where Date(day) {run_date}
and pool_address in (
--     '0x01cc44fc1246d17681b325926865cdb6242277a5',
--     '0x8981c60ff02cdbbf2a6ac1a9f150814f9cf68f62'
    select deposit_contract_address from `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a_poolInfo_deposit_contract`
)
),
deposit_with_address as (
    select deposit.*
    ,pool_info.token_address_list[offset(0)] as token0_address
    ,pool_info.token_address_list[offset(1)] as token1_address
    ,pool_info.name name
    from  deposit
    left join `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a_poolInfo_autoparse` pool_info on deposit.pool_address = pool_info.deposit_contract_address_list[offset(0)]
),
deposit_usd as(
    select d.*
    ,CAST(d.amount0 AS FLOAT64)*pow(10,-if(token0.decimals is null,18,token0.decimals))*p0.price + CAST(d.amount1 AS FLOAT64)*pow(10,-if(token1.decimals is null,18,token1.decimals))*p1.price AS usd_value
    from deposit_with_address d
    -- join token0
    left join `gaia-dao.gaia_dao.erc20_tokens_all`  token0
    on token0.token_address=d.token0_address and token0.chain = d.chain
    left join `gaia-dao.gaia_dao.token_price_daily_100d` p0
    on p0.token_address = d.token0_address and Date(d.block_timestamp)=p0.day and d.chain = p0.chain
    -- join token1
    left join `gaia-dao.gaia_dao.erc20_tokens_all`  token1
    on token1.token_address=d.token1_address and token1.chain = d.chain
    left join `gaia-dao.gaia_dao.token_price_daily_100d` p1
    on p1.token_address = d.token1_address and Date(d.block_timestamp)=p1.day and d.chain = p1.chain
)
select
day,chain,block_number,block_timestamp,transaction_hash,log_index,pool_address,token_id,amount0,amount1,usd_value,token0_address,token1_address,name
from deposit_usd
