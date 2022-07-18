from defi360.contracts_adapter.alpaca_finance.alpaca_finance_tvl import AlpacaFinancePoolBorrow
from utils.build_dag_util import BuildDAG

alpacaFinanceBorrow = AlpacaFinancePoolBorrow()
alpacaFinanceBorrow.project_id = 'gaia-data'
alpacaFinanceBorrow.data_set = 'gaia'
alpacaFinanceBorrow.depend_sql_path = 'defi360/contracts_adapter/alpaca_finance/alpaca_pools_gaia.sql'

DAG = BuildDAG().build_dag_with_ops(
    dag_params=alpacaFinanceBorrow.airflow_dag_params(),
    ops=alpacaFinanceBorrow.airflow_steps()
)