import pandas as pd

from defi360.common.python_adapter import PythonAdapter


class NewAddress(PythonAdapter):

    project_id = 'gaia-dao'
    data_set = 'gaia_dao'
    task_name = "protocol_new_address_indicator"
    execution_time = "5 5 * * *"
    schema_name = "defi360/schema/protocol_new_address_indicator.json"
    sql_path = "defi360/python_adapter/protocol_new_address.sql"



    def python_etl(self, df: pd.DataFrame):
        df = df.groupby(by=['protocol_slug', 'first_day'], as_index=True)['wallet_address'].count() \
            .reset_index().rename(columns={'wallet_address': 'count'})
        return df


if __name__ == '__main__':
    job = NewAddress()
    # # 执行 ETL
    job.run_etl(debug=False)
