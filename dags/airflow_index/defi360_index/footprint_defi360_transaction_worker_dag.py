import datetime

from defi360.transaction_worker.pool_info import PoolInfo
from defi360.transaction_worker.pool_parse_event import PoolParseEvent
from defi360.transaction_worker.pool_parse_transaction import PoolParseTransaction
from utils.build_dag_util import BuildDAG
from utils.constant import DEFI360_CHAIN

# 拆分是为了底层流水的问题. 因为目前为daily. 而BSC目前是稍微慢一点的.
# todo: 增加币价校验
BSC_DAG = BuildDAG().build_dag_with_ops(
    dag_params={
        "dag_id": "DeFi360_BSC_flow_worker_dag",
        "catchup": False,
        "schedule_interval": '0 3 * * *',
        "description": "BSC worker",
        "default_args": {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5),
            'start_date': datetime.datetime(2022, 1, 1)
        },
        "dagrun_timeout": datetime.timedelta(days=1),
        'tags': ['DeFi360', 'transaction']
    },
    ops=[
        PoolParseEvent(DEFI360_CHAIN['BSC']).parse_event,
        PoolInfo(DEFI360_CHAIN['BSC']).parse_pool_info,
        PoolParseTransaction(DEFI360_CHAIN['BSC']).parse_transaction
    ]
)

Ethereum_DAG = BuildDAG().build_dag_with_ops(
    dag_params={
        "dag_id": "DeFi360_Ethereum_flow_worker_dag",
        "catchup": False,
        "schedule_interval": '55 2 * * *',
        "description": "Ethereum worker",
        "default_args": {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5),
            'start_date': datetime.datetime(2022, 1, 1)
        },
        "dagrun_timeout": datetime.timedelta(days=1),
        'tags': ['DeFi360', 'transaction']
    },
    ops=[
        PoolParseEvent(DEFI360_CHAIN['ETHEREUM']).parse_event,
        PoolInfo(DEFI360_CHAIN['ETHEREUM']).parse_pool_info,
        PoolParseTransaction(DEFI360_CHAIN['ETHEREUM']).parse_transaction
    ]
)

Polygon_DAG = BuildDAG().build_dag_with_ops(
    dag_params={
        "dag_id": "DeFi360_Polygon_flow_worker_dag",
        "catchup": False,
        "schedule_interval": '55 2 * * *',
        "description": "Polygon worker",
        "default_args": {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5),
            'start_date': datetime.datetime(2022, 1, 1)
        },
        "dagrun_timeout": datetime.timedelta(days=1),
        'tags': ['DeFi360', 'transaction']
    },
    ops=[
        PoolParseEvent(DEFI360_CHAIN['POLYGON']).parse_event,
        PoolInfo(DEFI360_CHAIN['POLYGON']).parse_pool_info,
        PoolParseTransaction(DEFI360_CHAIN['POLYGON']).parse_transaction
    ]
)


Avalanche_DAG = BuildDAG().build_dag_with_ops(
    dag_params={
        "dag_id": "DeFi360_Avalanche_flow_worker_dag",
        "catchup": False,
        "schedule_interval": '55 2 * * *',
        "description": "Avalanche worker",
        "default_args": {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5),
            'start_date': datetime.datetime(2022, 1, 1)
        },
        "dagrun_timeout": datetime.timedelta(days=1),
        'tags': ['DeFi360', 'transaction']
    },
    ops=[
        PoolParseEvent(DEFI360_CHAIN['AVALANCHE']).parse_event,
        PoolInfo(DEFI360_CHAIN['AVALANCHE']).parse_pool_info,
        PoolParseTransaction(DEFI360_CHAIN['AVALANCHE']).parse_transaction
    ]
)
