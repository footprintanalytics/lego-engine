WITH Ethereum_parsed_traces AS
(SELECT
    transactions.block_timestamp AS block_timestamp
    ,transactions.block_number AS block_number
    ,transactions.from_address AS from_address
    ,transactions.to_address AS to_address
    ,`hash` AS transaction_hash
    ,`footprint-etl.Ethereum_izumi_autoparse.deltaNIZI_event_function`(input) AS parsed
FROM `bigquery-public-data.crypto_ethereum.transactions` AS transactions
WHERE to_address in ( select deposit_contract_address_list[OFFSET(0)] from `footprint-etl.Ethereum_izumi_autoparse.poolInfo` where defi_category="Farming")
  AND STARTS_WITH(transactions.input, '0xb36b9fa3') AND date(transactions.block_timestamp) > "2021-12-20"
  AND date(transactions.block_timestamp) {run_date}
  ),Polygon_parsed_traces AS
(SELECT
    transactions.block_timestamp AS block_timestamp
    ,transactions.block_number AS block_number
    ,transactions.from_address AS from_address
    ,transactions.to_address AS to_address
    ,`hash` AS transaction_hash
    ,`footprint-etl.Ethereum_izumi_autoparse.deltaNIZI_event_function`(input) AS parsed
FROM `footprint-blockchain-etl.crypto_polygon.transactions` AS transactions
WHERE to_address in ( select deposit_contract_address_list[OFFSET(0)] from `footprint-etl.Polygon_izumi_autoparse.poolInfo` where defi_category="Farming")
  AND STARTS_WITH(transactions.input, '0xb36b9fa3') AND date(transactions.block_timestamp) > "2022-01-11"
  AND date(transactions.block_timestamp) {run_date}
  )

-- boost --
,Ethereum_boost as (SELECT
	flow.*,
	info.pool_id,
	info.name,
	info.version,
    'iZi' AS token_symbol,
    token_amount_raw / POW(10, 18) AS token_amount,
FROM (
SELECT
        'Ethereum' as chain,
		date(block_timestamp) as day,
		block_number,
		block_timestamp,
		transaction_hash,
		contract_address,
		user as operator,
        safe_cast(tokenId as float64) as token_id,
		'0x9ad37205d608b8b219e6a2573f922094cec5c200' AS token_address,
		safe_cast(nIZI as float64) AS token_amount_raw
	FROM `footprint-etl.Ethereum_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256` where safe_cast(nIZI as float64) > 0 and date(block_timestamp)  {run_date}
union all
SELECT
        'Ethereum' as chain,
		date(block_timestamp) as day,
		block_number,
		block_timestamp,
		transaction_hash,
		to_address as contract_address,
		from_address as operator,
        safe_cast(tokenId as float64) as token_id,
		'0x9ad37205d608b8b219e6a2573f922094cec5c200' AS token_address,
		safe_cast(deltaNIZI as float64)  AS token_amount_raw,
	FROM (
        SELECT
            block_timestamp
            ,block_number
            ,transaction_hash,from_address,to_address
            ,parsed.error AS error
            ,parsed.tokenId AS `tokenId`
            ,parsed.deltaNIZI AS `deltaNIZI`
        FROM Ethereum_parsed_traces
    )

) flow left join `footprint-etl.Ethereum_izumi_autoparse.poolInfo` as info on info.deposit_contract_address_list[OFFSET(0)]=flow.contract_address and info.defi_category="Farming"
where pool_id is not null
)
-- boost --
,Polygon_boost as (SELECT
	flow.*,
	info.pool_id,
	info.name,
	info.version ,
    'iZi' AS token_symbol,
    token_amount_raw / POW(10, 18) AS token_amount,
FROM (
SELECT
        'Polygon' as chain,
		date(block_timestamp) as day,
		block_number,
		block_timestamp,
		transaction_hash,
		contract_address,
		user as operator,
        safe_cast(tokenId as float64) as token_id,
		'0x60d01ec2d5e98ac51c8b4cf84dfcce98d527c747' AS token_address,
		safe_cast(nIZI as float64) AS token_amount_raw
	FROM `footprint-etl.Polygon_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256` where safe_cast(nIZI as float64) > 0
	AND date(block_timestamp)   {run_date}
union all
SELECT
        'Polygon' as chain,
		date(block_timestamp) as day,
		block_number,
		block_timestamp,
		transaction_hash,
		to_address as contract_address,
		from_address as operator,
        safe_cast(tokenId as float64) as token_id,
		'0x60d01ec2d5e98ac51c8b4cf84dfcce98d527c747' AS token_address,
		safe_cast(deltaNIZI as float64) AS token_amount_raw,
	FROM (
        SELECT
            block_timestamp
            ,block_number
            ,transaction_hash,from_address,to_address
            ,parsed.error AS error
            ,parsed.tokenId AS `tokenId`
            ,parsed.deltaNIZI AS `deltaNIZI`
        FROM Polygon_parsed_traces
    )

) flow left join `footprint-etl.Polygon_izumi_autoparse.poolInfo` as info on info.deposit_contract_address_list[OFFSET(0)]=flow.contract_address and info.defi_category="Farming"
where pool_id is not null
)

SELECT s.*, s.token_amount * p.price usd_value FROM
(
    select * from Ethereum_boost union all
    select * from Polygon_boost
)s left join
`gaia-dao.gaia_dao.token_price` p on s.token_address = p.token_address and s.day=p.day
