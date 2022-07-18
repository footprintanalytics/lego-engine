SELECT `day`,`wallet_address`,`protocol_slug`,`chain` FROM `xed-project-237404.footprint_etl.protocol_monthly_address`
 where date(`day`) >=  date(date_sub(current_date,interval 1 year ))