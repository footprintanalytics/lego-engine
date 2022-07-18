from utils.build_dag_util import BuildDAG
from defi360.contracts_adapter.univ3.univ3_nft_positions import UNIV3NFTPositions

uniV3NFTPositions = UNIV3NFTPositions()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=uniV3NFTPositions.airflow_dag_params(),
    ops=uniV3NFTPositions.airflow_steps()
)
