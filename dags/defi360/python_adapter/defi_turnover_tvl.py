import pandas as pd

from defi360.common.python_adapter import PythonAdapter
from datetime import datetime


class DeFiTurnoverTVL(PythonAdapter):
    # 自己确定数据集
    project_id = "gaia-data"
    data_set = "origin_data"  # 默认是测试 set
    task_name = "defi_turnover_tvl"
    execution_time = "10 1 * * *"
    schema_name = "defi360/schema/defi_turnover_tvl.json"
    sql_path = "defi360/python_adapter/defi_turnover_tvl.sql"

    def python_etl(self, df: pd.DataFrame):
        df['volume'] = df['volume'].astype(float)
        df['tvl'] = df['tvl'].astype(float)
        df = df.set_index('day').sort_index()
        df = df.rename(columns={'slug': 'protocol_slug'})
        tvl_7d_avg = df.groupby(by=['protocol_id'], as_index=True)['tvl'].rolling(7).mean()
        volume_7d_summary = df.groupby(by=['protocol_id'], as_index=True)['volume'].rolling(7).sum()
        concat_data = pd.concat([tvl_7d_avg, volume_7d_summary], axis=1)
        concat_data = concat_data.rename(columns={'tvl': 'tvl_7d_avg', 'volume': 'volume_7d_summary'}).reset_index()
        new_df = concat_data.merge(df.reset_index(), on=['protocol_id', 'day'])
        new_df['turnover_tvl'] = new_df['volume_7d_summary'] / new_df['tvl_7d_avg']
        return new_df

    def get_bigquery_data_file_path(self):
        return self.build_file_path('../data/python_adapter/bigquery_data/{}_data.csv')

    def get_transfer_data_file_path(self):
        return self.build_file_path('../data/python_adapter/transfer_data/{}_data.csv')


if __name__ == '__main__':
    job = DeFiTurnoverTVL()
    # # 执行 ETL
    job.run_etl(debug=False)

