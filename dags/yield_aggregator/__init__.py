from yield_aggregator.aave.aave_pool import AavePool
from yield_aggregator.alchemix.alchemix_pool import AlchemixPool
from yield_aggregator.badger_finance.badger_pool import BadgerPool
from yield_aggregator.bprotocol.bprotocol_pool import BProtocol
from yield_aggregator.convex.convex_pool import ConvexPool
from yield_aggregator.cream_finance.cream_finance_pool import CreamFinancePool
from yield_aggregator.curve.curve_v1_pool import CurveV1Pool
from yield_aggregator.curve.curve_v2_pool import CurveV2Pool
from yield_aggregator.harvest.harvest_pool import HarvestPool
from yield_aggregator.idle.idle_pool import IdlePool
from yield_aggregator.liquity.liquity_pool import Liquity
from yield_aggregator.maker_dao.maker_dao_pool import MakerDAOPool
from yield_aggregator.pendle.pendle_pool import Pendle
from yield_aggregator.rari_capital.rari_capital_pool import RariCapitalPool
from yield_aggregator.trueFi.trueFi_pool import TrueFiPool
from yield_aggregator.yearn_finance.yearn_finance_pool import YearnFinancePool

if __name__ == '__main__':
    all_project = [
        AavePool(),
        AlchemixPool(),
        BadgerPool(),
        BProtocol(),
        ConvexPool(),
        CreamFinancePool(),
        CurveV1Pool(),
        CurveV2Pool(),
        HarvestPool(),
        IdlePool(),
        Liquity(),
        MakerDAOPool(),
        Pendle(),
        RariCapitalPool(),
        TrueFiPool(),
        YearnFinancePool()
    ]

    for project in all_project:
        for date_str in ['2021-11-06', '2021-11-07', '2021-11-08', '2021-11-09', '2021-11-10', '2021-11-11',
                         '2021-11-12', '2021-11-13', '2021-11-14']:
            project.run_daily_job(date_str=date_str)