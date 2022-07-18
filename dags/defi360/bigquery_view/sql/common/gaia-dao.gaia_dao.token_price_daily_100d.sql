SELECT
    *
FROM
  `gaia-dao.gaia_dao.token_price_daily_all`
WHERE
  DATE(day) > DATE_SUB(CURRENT_DATE(), INTERVAL 100 day)