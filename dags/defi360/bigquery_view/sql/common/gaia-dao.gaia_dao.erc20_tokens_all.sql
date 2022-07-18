select decimals, symbol, contract_address as token_address, 'Ethereum' as chain from `xed-project-237404.footprint_etl.erc20_tokens`
union all
select decimals, symbol, contract_address as token_address, 'BSC' as chain from `xed-project-237404.footprint_etl.bsc_erc20_tokens`
union all
select decimals, symbol, contract_address as token_address, 'Avalanche' as chain from `xed-project-237404.footprint_etl.avalanche_erc20_tokens`
union all
select decimals, symbol, contract_address as token_address, 'Fantom' as chain from `xed-project-237404.footprint_etl.fantom_erc20_tokens`
union all
select decimals, symbol, contract_address as token_address, 'Polygon' as chain from `xed-project-237404.footprint_etl.polygon_erc20_tokens`
union all
select decimals, symbol, contract_address as token_address, 'Arbitrum' as chain from `xed-project-237404.footprint_etl.arbitrum_erc20_tokens`