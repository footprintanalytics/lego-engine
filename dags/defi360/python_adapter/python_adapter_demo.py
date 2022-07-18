import pandas as pd

from defi360.common.python_adapter import PythonAdapter


class PythonAdapterDemo(PythonAdapter):
    # 自己确定数据集
    # data_set = "footprint-test-341610.gaia_dao_test" # 默认是测试 set
    task_name = "python_adapter_demo"
    execution_time = "30 2 * * *"
    schema_name = "defi360/schema/python_adapter_demo.json"
    sql_path = "defi360/python_adapter/python_adapter_demo.sql"

    def python_etl(self, df: pd.DataFrame):
        def upper_func(chain: str):
            return chain.upper()

        # 修改 df 后返回
        df['chain'] = df['chain'].apply(upper_func)
        return df


if __name__ == '__main__':
    job = PythonAdapterDemo()
    # 执行 ETL
    job.run_etl(debug=False)
