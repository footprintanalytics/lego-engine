--SynFutures Trades Flow on Ethereum
with eth_trade_event as
(
    SELECT block_timestamp, transaction_hash, trader, side,
    cast(price as float64) * pow(10, -18) as price,
    cast(size as float64) * pow(10, -18) as size,
    contract_address
    FROM `footprint-etl.Ethereum_SynFutures_autoparse.Trade_event_addressIndexed_uint8_uint256_uint256` WHERE DATE(block_timestamp) {run_date}
),
eth_create_event as(
    select (case when base = '0x0000000000000000000000000000000000000000' -- gas币使用wrap币替换，为了方便计算
     then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else base end) as base,
     cast(TIMESTAMP_SECONDS(cast(expiry as int64)) as date) as expiry,
     quote, oracle, amm, futures
      from `footprint-etl.Ethereum_SynFutures_autoparse.NewUniswapPair_event_address_address_address_uint256_address_address`
      -- 只挑了USDC
      where quote = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AND DATE(block_timestamp) {run_date}
),
eth_pool_info as (
    select deposit_contract_address_list[offset(0)] as contract_address, name, chain
    from `footprint-etl.Ethereum_SynFutures_autoparse.poolInfo`
),
eth_pool_name as (
    select base, expiry, quote, oracle, amm, futures, eth_pool_info.name, eth_pool_info.chain from eth_create_event
    inner join eth_pool_info
    on eth_create_event.futures = eth_pool_info.contract_address
),

eth_flow as (
    --少乘了quote的price
select *, price * size as trading_usd_volume from eth_pool_name
inner join eth_trade_event
on eth_trade_event.contract_address = eth_pool_name.futures
),
eth_token_symbol as (
    select t.*, a.symbol as baseSymbol, b.symbol as quoteSymbol from eth_flow  as t
    left join `xed-project-237404.footprint_etl.erc20_tokens` as a
    on t.base = a.contract_address
    left join `xed-project-237404.footprint_etl.erc20_tokens` as b
    on t.quote = b.contract_address
),




--SynFutures Trades Flow on Polygon
polygon_trade_event as
(
    SELECT block_timestamp, transaction_hash, trader, side,
    cast(price as float64) * pow(10, -18) as price,
    cast(size as float64) * pow(10, -18) as size,
    contract_address
    FROM `footprint-etl.Polygon_SynFutures_autoparse.Trade_event_addressIndexed_uint8_uint256_uint256` WHERE DATE(block_timestamp) {run_date}
),
polygon_create_event as(
    select (case when base = '0x0000000000000000000000000000000000000000'  -- gas币使用wrap币替换，为了方便计算
     then '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' else base end) as base,
     cast(TIMESTAMP_SECONDS(cast(expiry as int64)) as date) as expiry,
     quote, oracle, amm, futures
      from `footprint-etl.Polygon_SynFutures_autoparse.NewUniswapPair_event_address_address_address_uint256_address_address`
      -- 只挑了USDC
      where quote = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' AND DATE(block_timestamp) {run_date}
),
polygon_pool_info as (
    select deposit_contract_address_list[offset(0)] as contract_address, name, chain
    from `footprint-etl.Polygon_SynFutures_autoparse.poolInfo`
),
polygon_pool_name as (
    select base, expiry, quote, oracle, amm, futures, polygon_pool_info.name, polygon_pool_info.chain from polygon_create_event
    inner join polygon_pool_info
    on polygon_create_event.futures = polygon_pool_info.contract_address
),
polygon_flow as (
    --少乘了quote的price
    select *, price * size as trading_usd_volume from polygon_pool_name
    inner join polygon_trade_event
    on polygon_trade_event.contract_address = polygon_pool_name.futures
),
polygon_token_symbol as (
    select t.*, a.symbol as baseSymbol, b.symbol as quoteSymbol from polygon_flow as t
    left join `xed-project-237404.footprint_etl.polygon_erc20_tokens` as a
    on t.base = a.contract_address
    left join `xed-project-237404.footprint_etl.polygon_erc20_tokens` as b
    on t.quote = b.contract_address
),


--SynFutures Trades Flow on BSC

bsc_trade_event as
(
    SELECT block_timestamp, transaction_hash, trader, side,
    cast(price as float64) * pow(10, -18) as price,
    cast(size as float64) * pow(10, -18) as size,
    contract_address
    FROM `footprint-etl.BSC_SynFutures_autoparse.Trade_event_addressIndexed_uint8_uint256_uint256` WHERE DATE(block_timestamp) {run_date}
),
bsc_create_event as(
    select (case when base = '0x0000000000000000000000000000000000000000'  -- gas币使用wrap币替换，为了方便计算
     then '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' else base end) as base,
     cast(TIMESTAMP_SECONDS(cast(expiry as int64)) as date) as expiry,
     quote, oracle, amm, futures
      from `footprint-etl.BSC_SynFutures_autoparse.NewUniswapPair_event_address_address_address_uint256_address_address`
      -- 只挑了BUSD
      where quote = '0xe9e7cea3dedca5984780bafc599bd69add087d56' AND DATE(block_timestamp) {run_date}
),
bsc_pool_info as (
    select deposit_contract_address_list[offset(0)] as contract_address, name, chain
    from `footprint-etl.BSC_SynFutures_autoparse.poolInfo`
),
bsc_pool_name as (
    select base, expiry, quote, oracle, amm, futures, bsc_pool_info.name, bsc_pool_info.chain from bsc_create_event
    inner join bsc_pool_info
    on bsc_create_event.futures = bsc_pool_info.contract_address
),
bsc_flow as (
    --少乘了quote的price
select *, price * size as trading_usd_volume from bsc_pool_name
inner join bsc_trade_event
on bsc_trade_event.contract_address = bsc_pool_name.futures
),
bsc_token_symbol as (
    select t.*, a.symbol as baseSymbol, b.symbol as quoteSymbol from bsc_flow as t
    left join `xed-project-237404.footprint_etl.bsc_erc20_tokens` as a
    on t.base = a.contract_address
    left join `xed-project-237404.footprint_etl.bsc_erc20_tokens` as b
    on t.quote = b.contract_address
)

select * from eth_token_symbol
union all
select * from polygon_token_symbol
union all
select * from bsc_token_symbol