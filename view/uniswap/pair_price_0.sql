
(
SELECT
block_number,
token1 as token,
avg(price0) as price
FROM `xed-project-237404.footprint_test.uni_price_base`
WHERE
token0 in (
    '0x6b175474e89094c44da98b954eedeac495271d0f',
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    '0xdac17f958d2ee523a2206206994597c13d831ec7',
    '0x0000000000085d4780b73119b644ae5ecd22b376')
group by block_number, token1
)
union all
(
SELECT
block_number,
token0 as token,
avg(price1) as price
FROM `xed-project-237404.footprint_test.uni_price_base`
WHERE
token1 in (
    '0x6b175474e89094c44da98b954eedeac495271d0f',
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    '0xdac17f958d2ee523a2206206994597c13d831ec7',
    '0x0000000000085d4780b73119b644ae5ecd22b376')
group by block_number, token0
)