import pandas as pd

from defi360.common.python_adapter import PythonAdapter


class NewAddress(PythonAdapter):

    project_id = 'gaia-data'
    data_set = 'gaia'
    task_name = "ud_protocol_new_address_indicator"
    execution_time = "5 5 * * *"
    schema_name = "defi360/schema/protocol_new_address_indicator.json"
    sql_path = "defi360/python_adapter/ud_protocol_new_address.sql"

    def python_etl(self, df: pd.DataFrame):
        df = df.groupby(by=['protocol_slug', 'first_day'], as_index=True)['wallet_address'].count() \
            .reset_index().rename(columns={'wallet_address': 'count'})
        return df


if __name__ == '__main__':
    job = NewAddress()
    # # 执行 ETL
    job.run_etl(debug=False)