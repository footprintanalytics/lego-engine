from datetime import timedelta, datetime
from common.common_dex.common_dex_swap import CommonDexSwap
from common.common_dex.common_dex_add_liquidity import CommonDexAddLiquidity
from common.common_dex.common_dex_remove_liquidity import CommonDexRemoveLiquidity


class DexModel:
    project_id = 'footprint-etl'
    project_name = 'common_dex'
    task_name = 'common_dex'
    execution_time = '5 3 * * *'
    model_type: str = 'dex_model'
    skip_liquidity = False
    history_date = '2021-11-03'

    source_add_liquidity_sql_file = None
    source_remove_liquidity_sql_file = None
    source_swap_sql_file = None

    dex_swap_schema = 'dex_swap'
    dex_add_liquidity_schema = 'dex_add_liquidity'
    dex_remove_liquidity_schema = 'dex_remove_liquidity'

    Swap: CommonDexSwap = None
    AddLiquidity: CommonDexAddLiquidity = None
    RemoveLiquidity: CommonDexRemoveLiquidity = None

    business_type: dict = {
        'swap': 'swap',
        'add_liquidity': 'add_liquidity',
        'remove_liquidity': 'remove_liquidity'
    }

    def __init__(self):
        self.Swap = CommonDexSwap().init(
            self.project_id,
            self.project_name,
            self.task_name + '_swap',
            self.model_type,
            self.dex_swap_schema,
            self.source_swap_sql_file,
            self.history_date
        )

        if (self.source_remove_liquidity_sql_file is None) or (self.source_add_liquidity_sql_file is None):
            self.skip_liquidity = True

        if self.skip_liquidity is False:
            self.AddLiquidity = CommonDexAddLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_add_liquidity',
                self.model_type,
                self.dex_add_liquidity_schema,
                self.source_add_liquidity_sql_file,
                self.history_date
            )
            self.RemoveLiquidity = CommonDexRemoveLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_remove_liquidity',
                self.model_type,
                self.dex_remove_liquidity_schema,
                self.source_remove_liquidity_sql_file,
                self.history_date
            )

    def get_business_type(self, key: str):
        if self.business_type[key] is self.business_type['swap']:
            return self.Swap
        if self.business_type[key] is self.business_type['add_liquidity']:
            return self.AddLiquidity
        if self.business_type[key] is self.business_type['remove_liquidity']:
            return self.RemoveLiquidity

    def airflow_steps(self):
        steps = list(map(lambda f: {'type': 'swap', 'func': f}, self.Swap.airflow_steps()))

        if self.skip_liquidity is False:
            steps = [
                *list(map(lambda f: {'type': 'swap', 'func': f}, self.Swap.airflow_steps())),
                *list(map(lambda f: {'type': 'add_liquidity', 'func': f}, self.AddLiquidity.airflow_steps())),
                *list(map(lambda f: {'type': 'remove_liquidity', 'func': f}, self.RemoveLiquidity.airflow_steps()))
            ]
        return steps

    def airflow_dag_params(self):
        dag_params = {
            "dag_id": "footprint_{}_{}_dag".format(self.model_type, self.task_name),
            "catchup": False,
            "schedule_interval": self.execution_time,
            "description": "{}_dag".format(self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 8, 20)
            },
            "dagrun_timeout": timedelta(days=30),
            "tags": ["dex_transactions"]
        }
        print('dag_params', dag_params)
        return dag_params

    def get_daily_table_name(self, key: str):
        if self.business_type[key] is self.business_type['swap']:
            return self.Swap.get_daily_table_name()
        if self.business_type[key] is self.business_type['add_liquidity']:
            return self.AddLiquidity.get_daily_table_name()
        if self.business_type[key] is self.business_type['remove_liquidity']:
            return self.RemoveLiquidity.get_daily_table_name()

    def get_history_table_name(self, key: str):
        if self.business_type[key] is self.business_type['swap']:
            return self.Swap.get_history_table_name()
        if self.business_type[key] is self.business_type['add_liquidity']:
            return self.AddLiquidity.get_history_table_name()
        if self.business_type[key] is self.business_type['remove_liquidity']:
            return self.RemoveLiquidity.get_history_table_name()
