select
  dds.*,
  tvdsv.trading_volume as volume
from
  `xed-project-237404.footprint_etl.defi_daily_stats` dds
  left join
  `xed-project-237404.footprint_etl.trading_volume_daily_stats_view` tvdsv
  on Date(tvdsv.day) = Date(dds.day)
  and tvdsv.protocol_id = dds.protocol_id
