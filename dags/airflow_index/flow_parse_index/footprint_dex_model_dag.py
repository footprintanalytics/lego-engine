from dex.ethereum.airswap.airswap import airswapDex
from dex.ethereum.balancer.balancer import BalancerDex
from dex.ethereum.dydx.dydx import dydxDex
from dex.ethereum.idex.idex import idexDex
from dex.ethereum.mooniswap.mooniswap import mooniswapDex
from dex.ethereum.paraswap.paraswap import paraswapDex
from dex.ethereum.tokenlon.tokenlon import TokenlonDex
from dex.ethereum.sushi.sushi import SushiDex
from dex.ethereum.swapr.swapr import swaprDex
from dex.ethereum.curve.curve import Curve
from dex.ethereum.kyber.kyber import kyberDex
from dex.ethereum.uniswap.uniswap import UniswapDex
from dex.ethereum.oasis.oasis import OasisDex
from dex.ethereum.synthetix.synthetix import SynthetixDex
from dex.ethereum.bancor.bancor import bancorDex
from dex.ethereum.gnosis_protocol.gnosis_protocol import gnosis_protocolDex
from dex.ethereum.linkswap.linkswap import linkswapDex
from dex.ethereum.mistx.mistx import mistxDex
from dex.ethereum.dodo.dodo import DodoDex
from dex.polygon.quickswap.quickswap import QuickswapDex
from dex.polygon.sushi.sushi import SushiDex as PolygonSushiDex
from dex.ethereum.clipper.clipper import ClipperDex
from dex.ethereum.zeroex.zeroex import ZeroexDex
from dex.bsc.biswap.biswap import BiswapDex
from dex.bsc.ellipsis.ellipsis import Ellipsis
from dex.bsc.mdex.mdex import MdexDex
from dex.ethereum.shibaswap.shibaswap import ShibaswapDex
from dex.bsc.apeswap.apeswap import ApeswapDex
from dex.bsc.pancakeswap.pancakeswap import PancakeSwapDex
from dex.avalanche.traderjoe.traderjoe import TraderjoeDex
from dex.avalanche.pangolin.pangolin import PangolinDex
from dex.arbitrum.sushi.sushi import SushiDex as ArbitrumSushiDex
from dex.fantom.spookyswap.spookyswap import SpookySwapDex
from utils.build_dag_util import BuildDAG
from datetime import timedelta

all_project = [
    BalancerDex(),
    UniswapDex(),
    SushiDex(),
    mooniswapDex(),
    airswapDex(),
    idexDex(),
    dydxDex(),
    swaprDex(),
    Curve(),
    kyberDex(),
    TokenlonDex(),
    SynthetixDex(),
    OasisDex(),
    bancorDex(),
    gnosis_protocolDex(),
    linkswapDex(),
    mistxDex(),
    DodoDex(),
    QuickswapDex(),
    PolygonSushiDex(),
    ClipperDex(),
    paraswapDex(),
    ZeroexDex(),
    Ellipsis(),
    MdexDex(),
    BiswapDex(),
    ShibaswapDex(),
    ApeswapDex(),
    PancakeSwapDex(),
    TraderjoeDex(),
    PangolinDex(),
    ArbitrumSushiDex(),
    SpookySwapDex()
]

def build_airflow_steps(steps: list):
    def build_task_params(type, method):
        return {
            "task_id": '{}_{}'.format(type, method.__name__),
            "python_callable": method,
            "execution_timeout": timedelta(days=30)
        }

    dag_task_params = list(map(lambda n: build_task_params(n.get('type'), n.get('func')), steps))
    print('python_operator ', dag_task_params)
    return dag_task_params


for project in all_project:
    dag_id = "footprint_dex_model_{}_dag".format(project.task_name)
    globals()[dag_id] = BuildDAG().build_dag(
        dag_params=project.airflow_dag_params(),
        dag_task_params=build_airflow_steps(project.airflow_steps())
    )
