SELECT
  chain,
  operator,
  contract_address,
  safe_cast(token_amount_raw as INTEGER ) as token_id,
  block_number
FROM
  `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a`

WHERE  business_type = 'farmingSupply'
and token_symbol = 'Unit NFT'