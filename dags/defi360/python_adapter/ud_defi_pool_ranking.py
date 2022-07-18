import pandas as pd
from defi360.common.python_adapter import PythonAdapter
from defi360.python_adapter.util.csv_util import fill_day


class DeFiPoolRinking(PythonAdapter):
    # 自己确定数据集
    project_id = "gaia-data"
    data_set = "gaia"  # 默认是测试 set
    task_name = "ud_defi_pool_ranking"
    execution_time = "0 5 * * *"
    schema_name = "defi360/schema/defi_pool_ranking.json"
    sql_path = "defi360/python_adapter/defi_pool_ranking_v2.sql"

    def calculate_ranking(self, origin_df: pd.DataFrame):
        filled_day_df = fill_day(origin_df)
        filled_day_df = filled_day_df.fillna({'current_value': 0}).fillna(method='pad').dropna(subset=['pool_id'])
        days = [7, 15, 30]
        for day in days:
            new_column = f'value_{day}d_ago'
            filled_day_df[new_column] = filled_day_df['current_value'].shift(day).fillna(0)
        filled_day_df.fillna(0, inplace=True)
        return filled_day_df

    def python_etl(self, df: pd.DataFrame):
        df['day'] = pd.to_datetime(df['day'])
        df = df.groupby(['pool_id', 'business_type']).apply(self.calculate_ranking)
        return df


if __name__ == '__main__':
    job = DeFiPoolRinking()

    # # 执行 ETL
    job.run_etl(debug=False)
