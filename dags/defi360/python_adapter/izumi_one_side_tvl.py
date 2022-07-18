import pandas as pd
from defi360.common.python_adapter import PythonAdapter
from defi360.python_adapter.util.csv_util import fill_day


class IzumiOneSideTvl(PythonAdapter):
    # 自己确定数据集
    project_id = "gaia-data"
    data_set = "gaia"  # 默认是测试 set
    task_name = "izumi_one_side_tvl"
    execution_time = "0 5 * * *"
    schema_name = "defi360/schema/izumi_one_side_tvl.json"
    sql_path = "defi360/python_adapter/izumi_one_side_tvl.sql"

    def fill_pool_day(self, one_pool: pd.DataFrame):
        filled_day_df = fill_day(one_pool)
        filled_day_df = filled_day_df.fillna(method='pad').dropna(subset=['contract_address'])
        return filled_day_df

    def python_etl(self, df: pd.DataFrame):
        df = df.sort_values(by=['day', 'token_amount'], ascending=[True, False], ignore_index=True).fillna({'token': 0})
        df['tvl'] = df.loc[:, ['contract_address', 'token_amount']].groupby(['contract_address']).cumsum()
        df['day'] = pd.to_datetime(df['day'])
        result = df.groupby(['contract_address']).apply(self.fill_pool_day)
        return result


if __name__ == '__main__':
    job = IzumiOneSideTvl()
    # # 执行 ETL
    job.run_etl(debug=False)
