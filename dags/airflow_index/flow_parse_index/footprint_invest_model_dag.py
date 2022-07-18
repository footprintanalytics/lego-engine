from utils.build_dag_util import BuildDAG
from yield_aggregator.compound.compound_pool import CompoundPool
from yield_aggregator.curve.curve_v1_pool import CurveV1Pool
from yield_aggregator.idle.idle_pool import IdlePool
from yield_aggregator.bprotocol.bprotocol_pool import BProtocol
from yield_aggregator.pendle.pendle_pool import Pendle
from yield_aggregator.convex.convex_pool import ConvexPool
from yield_aggregator.liquity.liquity_pool import Liquity
from yield_aggregator.aave.aave_pool import AavePool
from yield_aggregator.alchemix.alchemix_pool import AlchemixPool
from yield_aggregator.yearn_finance.yearn_finance_pool import YearnFinancePool
from yield_aggregator.maker_dao.maker_dao_pool import MakerDAOPool
from yield_aggregator.badger_finance.badger_pool import BadgerPool
from yield_aggregator.trueFi.trueFi_pool import TrueFiPool
from yield_aggregator.harvest.harvest_pool import HarvestPool
from yield_aggregator.cream_finance.cream_finance_pool import CreamFinancePool
from yield_aggregator.rari_capital.rari_capital_pool import RariCapitalPool
from yield_aggregator.curve.curve_v2_pool import CurveV2Pool

# deprecated
# idle = IdlePool()
# DAG1 = BuildDAG().build_dag_with_ops(dag_params=idle.airflow_dag_params(), ops=idle.airflow_steps())
#
# compound = CompoundPool()
# DAG2 = BuildDAG().build_dag_with_ops(dag_params=compound.airflow_dag_params(), ops=compound.airflow_steps())
#
# bprotocol = BProtocol()
# DAG3 = BuildDAG().build_dag_with_ops(dag_params=bprotocol.airflow_dag_params(), ops=bprotocol.airflow_steps())
#
# pendle = Pendle()
# DAG4 = BuildDAG().build_dag_with_ops(dag_params=pendle.airflow_dag_params(), ops=pendle.airflow_steps())
#
# curve_v1 = CurveV1Pool()
# DAG5 = BuildDAG().build_dag_with_ops(dag_params=curve_v1.airflow_dag_params(), ops=curve_v1.airflow_steps())
#
# convex = ConvexPool()
# DAG6 = BuildDAG().build_dag_with_ops(dag_params=convex.airflow_dag_params(), ops=convex.airflow_steps())
#
# liquity = Liquity()
# DAG7 = BuildDAG().build_dag_with_ops(dag_params=liquity.airflow_dag_params(), ops=liquity.airflow_steps())
#
# aavePool = AavePool()
# DAG8 = BuildDAG().build_dag_with_ops(dag_params=aavePool.airflow_dag_params(), ops=aavePool.airflow_steps())
#
# alchemixPool = AlchemixPool()
# DAG9 = BuildDAG().build_dag_with_ops(dag_params=alchemixPool.airflow_dag_params(), ops=alchemixPool.airflow_steps())
#
# yearnFinancePool = YearnFinancePool()
# DAG10 = BuildDAG().build_dag_with_ops(dag_params=yearnFinancePool.airflow_dag_params(), ops=yearnFinancePool.airflow_steps())
#
# badgerPool = BadgerPool()
# DAG11 = BuildDAG().build_dag_with_ops(dag_params=badgerPool.airflow_dag_params(), ops=badgerPool.airflow_steps())
#
# trueFiPool = TrueFiPool()
# DAG12 = BuildDAG().build_dag_with_ops(dag_params=trueFiPool.airflow_dag_params(), ops=trueFiPool.airflow_steps())
#
# harvestPool = HarvestPool()
# DAG13 = BuildDAG().build_dag_with_ops(dag_params=harvestPool.airflow_dag_params(), ops=harvestPool.airflow_steps())
#
# makerDAOPool = MakerDAOPool()
# DAG14 = BuildDAG().build_dag_with_ops(dag_params=makerDAOPool.airflow_dag_params(), ops=makerDAOPool.airflow_steps())
#
# creamFinancePool = CreamFinancePool()
# DAG15 = BuildDAG().build_dag_with_ops(dag_params=creamFinancePool.airflow_dag_params(), ops=creamFinancePool.airflow_steps())
#
# curveV2Pool = CurveV2Pool()
# DAG16 = BuildDAG().build_dag_with_ops(dag_params=curveV2Pool.airflow_dag_params(), ops=curveV2Pool.airflow_steps())
#
# rariCapitalPool = RariCapitalPool()
# DAG17 = BuildDAG().build_dag_with_ops(dag_params=rariCapitalPool.airflow_dag_params(), ops=rariCapitalPool.airflow_steps())