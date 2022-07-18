import pandas as pd
from defi360.common.python_adapter import PythonAdapter
import operator as op
import numpy as np


class ProtocolDailyStats(PythonAdapter):
    # 自己确定数据集
    project_id = 'xed-project-237404'
    data_set = 'footprint_etl'
    task_name = "protocol_daily_stats"
    execution_time = "50 3 * * *"
    schema_name = "defi360/schema/protocol_daily_stats.json"
    sql_path = "defi360/python_adapter/protocol_daily_stats.sql"
    category = 'gamefi'

    def python_etl(self, df: pd.DataFrame):
        # 特殊处理，保证最后一天所有人都有数据，用最后一次出现来补
        # 拿到最后有值的那个时间的平台
        address_last = df.groupby(['chain', 'protocol_slug'], as_index=False).last()
        trove_all = pd.concat([df, address_last], axis=0)
        # 按天采样，取当天最后一笔值
        trove_all['day'] = pd.to_datetime(trove_all.day, format='%Y-%m-%d')
        trove_daily = trove_all.set_index('day').groupby(['chain', 'protocol_slug','protocol_name'], as_index=True).resample('d')[['new_users', 'unique_users','total_users']].last()
        trove_daily = trove_daily.reset_index()
        # 补齐
        trove_daily_filled = trove_daily.groupby(by=['protocol_slug', 'chain'])[['day', 'new_users', 'unique_users','total_users']].fillna(0)
        trove_daily_filled['chain'] = trove_daily['chain']
        trove_daily_filled['protocol_slug'] = trove_daily['protocol_slug']
        trove_daily_filled['protocol_name'] = trove_daily['protocol_name']
        for i in [1, 7, 30, 180, 360]:
            key_name = 'new_users_' + str(i) + 'd_pct_change'
            trove_daily_filled[key_name] = trove_daily_filled['new_users'].pct_change(i).round(4)
        for i in [1, 7, 30, 180, 360]:
            key_name = 'active_users_' + str(i) + 'd_pct_change'
            trove_daily_filled[key_name] = trove_daily_filled['unique_users'].pct_change(i).round(4)
        ptc_change_fill = trove_daily_filled.replace(np.inf, np.nan)
        ptc_change_fill.fillna(0, inplace=True)
        return ptc_change_fill


if __name__ == '__main__':
    job = ProtocolDailyStats()
    # # 执行 ETL
    job.run_etl(debug=False)
