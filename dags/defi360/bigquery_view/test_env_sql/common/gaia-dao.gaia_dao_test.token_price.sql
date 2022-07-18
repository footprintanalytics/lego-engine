SELECT
  token_address,
  TIMESTAMP(day) AS day,
  price,
  chain
FROM
  `footprint-etl-internal.view_to_table.token_daily_price`
WHERE
  DATE(day) > DATE_SUB(CURRENT_DATE(), INTERVAL 100 day)