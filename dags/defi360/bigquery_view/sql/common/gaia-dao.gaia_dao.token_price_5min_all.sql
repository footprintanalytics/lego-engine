SELECT
  address AS token_address,
  timestamp,
  price,
  (CASE
      WHEN chain='Binance' THEN 'BSC'
    ELSE
    chain
  END
    ) AS chain
FROM
  `footprint-etl-internal.view_to_table.fixed_price`