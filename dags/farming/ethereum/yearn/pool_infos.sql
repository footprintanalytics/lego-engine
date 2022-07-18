with valuts as (
    select block_timestamp, vault, token FROM `footprint-etl.ethereum_yearn.NewExperimentalVault_event_NewExperimentalVault` group by 1, 2, 3
),
-- full_valuts as (
--     select * from valuts
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0x671a912c10bba0cfa74cfc2d6fba9ba1ed9530b2' as vault, '0x514910771af9ca656af840dff83e8264ecf986ca' as token
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0x986b4aff588a109c09b50a03f42e4110e29d353f' as vault, '0xa3d87fffce63b53e0d54faa1cc983b7eb0b74a9c' as token
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0xa696a63cc78dffa1a63e9e50587c197387ff6c7e' as vault, '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599' as token
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0xe11ba472f74869176652c35d30db89854b5ae84d' as vault, '0x584bc13c7d411c00c01a62e8019472de68768430' as token
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0xcb550a6d4c8e3517a939bc79d0c7093eb7cf56b5' as vault, '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599' as token
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0xbfa4d8aa6d8a379abfe7793399d3ddacc5bbecbb' as vault, '0x6b175474e89094c44da98b954eedeac495271d0f' as token
--     union all
--     select TIMESTAMP("2021-01-03 12:34:56+00") as block_timestamp, '0xe2f6b9773bf3a015e2aa70741bde1498bdb9425b' as vault, '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as token
-- ),
valut_infos as (
    select * from valuts left join  `xed-project-237404.footprint_etl.erc20_tokens` as a  on valuts.token = a.contract_address
)

select
    vault as pool_id,
    9 as protocol_id,
    'Yearn' AS project,
    "Ethereum" AS chain,
    'farming' as business_type,
    vault as  deposit_contract,
    vault as  withdraw_contract,
    '' as lp_token,
    CAST([token] AS ARRAY<string>)  as stake_token,
    CAST(NULL AS ARRAY<string>) AS stake_underlying_token,
    CAST([token] AS ARRAY<string>) AS reward_token,
    symbol as name,
    '' AS description,
from valut_infos
where symbol is not null
and block_timestamp {match_date_filter}
