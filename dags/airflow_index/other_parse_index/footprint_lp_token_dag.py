from token_stats.lp_token_price.lp_token_price import LPTokenPrice
from utils.build_dag_util import BuildDAG

lp_token = LPTokenPrice()

DAG = BuildDAG().build_dag_with_ops(dag_params=lp_token.airflow_dag_params(), ops=lp_token.airflow_steps())

from token_stats.dex_trades_token_price import EthereumDexTradesPrice, BscDexTradesTokenPrice, \
    PolygonDexTradesTokenPrice, AvalancheDexTradesTokenPrice, ArbitrumDexTradesTokenPrice, FantomDexTradesTokenPrice
from token_stats.dex_trades_token_price.fixed_price import create_dag

DAG_POLYGON = create_dag(instance=PolygonDexTradesTokenPrice())
DAG_ETHEREUM = create_dag(instance=EthereumDexTradesPrice(), is_fixed=True)
DAG_BSC = create_dag(instance=BscDexTradesTokenPrice(), is_fixed=True)
DAG_AVALANCHE = create_dag(instance=AvalancheDexTradesTokenPrice())
DAG_ARBITRUM = create_dag(instance=ArbitrumDexTradesTokenPrice())
DAG_FANTOM = create_dag(instance=FantomDexTradesTokenPrice())
