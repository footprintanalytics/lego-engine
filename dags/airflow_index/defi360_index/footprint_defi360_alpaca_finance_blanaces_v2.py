from defi360.contracts_adapter.alpaca_finance.alpaca_finance_tvl import AlpacaFinancePoolBalance
from utils.build_dag_util import BuildDAG

alpacaFinanceBalance = AlpacaFinancePoolBalance()
alpacaFinanceBalance.project_id = 'gaia-data'
alpacaFinanceBalance.data_set = 'gaia'
alpacaFinanceBalance.depend_sql_path = 'defi360/contracts_adapter/alpaca_finance/alpaca_pools_gaia.sql'

DAG = BuildDAG().build_dag_with_ops(
    dag_params=alpacaFinanceBalance.airflow_dag_params(),
    ops=alpacaFinanceBalance.airflow_steps()
)