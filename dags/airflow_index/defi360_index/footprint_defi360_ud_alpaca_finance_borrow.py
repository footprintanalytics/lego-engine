from defi360.contracts_adapter.alpaca_finance.ud_alpaca_finance_tvl import AlpacaFinancePoolBorrow
from utils.build_dag_util import BuildDAG

alpacaFinanceBorrow = AlpacaFinancePoolBorrow()


DAG = BuildDAG().build_dag_with_ops(
    dag_params=alpacaFinanceBorrow.airflow_dag_params(),
    ops=alpacaFinanceBorrow.airflow_steps()
)