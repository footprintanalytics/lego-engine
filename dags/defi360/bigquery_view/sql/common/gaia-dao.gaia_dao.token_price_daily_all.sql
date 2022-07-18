SELECT
  token_address,
  day,
  price,
  (CASE
      WHEN chain='Binance' THEN 'BSC'
    ELSE
    chain
  END
    ) AS chain
FROM
  `footprint-etl-internal.view_to_table.token_daily_price`