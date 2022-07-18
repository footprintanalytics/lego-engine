SELECT
 *
FROM
  `gaia-dao.gaia_dao.token_price_5min_all`
WHERE
  DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 100 day)