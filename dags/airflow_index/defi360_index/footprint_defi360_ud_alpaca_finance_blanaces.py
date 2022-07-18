from defi360.contracts_adapter.alpaca_finance.ud_alpaca_finance_tvl import AlpacaFinancePoolBalance
from utils.build_dag_util import BuildDAG

alpacaFinanceBalance = AlpacaFinancePoolBalance()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=alpacaFinanceBalance.airflow_dag_params(),
    ops=alpacaFinanceBalance.airflow_steps()
)