from copy import deepcopy
from common.common_dex_model import DexModel
from datetime import datetime,timedelta

from dex.ethereum.balancer.balancer import BalancerDex
from dex.ethereum.dodo.dodo import DodoDex
from dex.ethereum.dydx.dydx import dydxDex
from dex.ethereum.idex.idex import idexDex
from dex.ethereum.oasis.oasis import OasisDex
from dex.ethereum.paraswap.paraswap import paraswapDex
from dex.ethereum.sushi.sushi import SushiDex
from dex.ethereum.swapr.swapr import swaprDex
from dex.ethereum.uniswap.uniswap import UniswapDex
from common.pool_all_data_view_builder import AllDataViewBuilder
from dex.ethereum.mooniswap.mooniswap import mooniswapDex
from dex.ethereum.airswap.airswap import airswapDex
from dex.ethereum.curve.curve import Curve
from dex.ethereum.kyber.kyber import kyberDex
from dex.ethereum.synthetix.synthetix import SynthetixDex
from dex.ethereum.bancor.bancor import bancorDex
from dex.ethereum.gnosis_protocol.gnosis_protocol import gnosis_protocolDex
from dex.ethereum.linkswap.linkswap import linkswapDex
from dex.ethereum.mistx.mistx import mistxDex
from dex.polygon.sushi.sushi import SushiDex as polygonSuShiDex
from dex.polygon.quickswap.quickswap import QuickswapDex
from dex.ethereum.clipper.clipper import ClipperDex
from dex.ethereum.tokenlon.tokenlon import TokenlonDex
from dex.ethereum.zeroex.zeroex import ZeroexDex
from dex.bsc.biswap.biswap import BiswapDex
from dex.bsc.ellipsis.ellipsis import Ellipsis
from dex.bsc.mdex.mdex import MdexDex
from dex.ethereum.shibaswap.shibaswap import ShibaswapDex
from dex.bsc.pancakeswap.pancakeswap import PancakeSwapDex
from dex.bsc.apeswap.apeswap import ApeswapDex
from utils.query_bigquery import query_bigquery
from dex.avalanche.traderjoe.traderjoe import TraderjoeDex
from dex.avalanche.pangolin.pangolin import PangolinDex
from dex.arbitrum.sushi.sushi import SushiDex as ArbitrumSuShiDex
from dex.fantom.spookyswap.spookyswap import SpookySwapDex
import pydash


all_project = [
    BalancerDex(),
    Curve(),
    UniswapDex(),
    SushiDex(),
    SynthetixDex(),
    bancorDex(),
    MdexDex(),
    QuickswapDex(),
    Ellipsis(),
    BiswapDex(),
    ShibaswapDex(),
    polygonSuShiDex(),
    DodoDex(),
    gnosis_protocolDex(),
    ClipperDex(),
    idexDex(),
    TokenlonDex(),
    swaprDex(),
    mooniswapDex(),
    linkswapDex(),
    mistxDex(),
    airswapDex(),
    paraswapDex(),
    OasisDex(),
    ZeroexDex(),
    kyberDex(),
    ApeswapDex(),
    PancakeSwapDex(),
    TraderjoeDex(),
    PangolinDex(),
    ArbitrumSuShiDex(),
    SpookySwapDex()
]

def dex_liquidity_view_warp():
    return """
        select
        t.project,
        info.chain,
        t.liquidity_provider,
        t.version,
        t.protocol_id,
        t.token_symbol,
        t.token_amount,
        -- SAFE_MULTIPLY(a.price, t.token_amount) as usd_value_of_token,
        t.token_amount_raw,
        t.type,
        t.token_address,
        t.exchange_address,
        t.tx_hash,
        t.block_time,
        t.block_number,
        t.tx_from,
        -- a.price
        from ({dex_source}) t
        left join `xed-project-237404.footprint_etl.defi_protocol_info` info
        on info.protocol_id = t.protocol_id
        -- left join  `footprint-etl-internal.view_to_table.fixed_price`a 
        -- on lower(t.token_a_address) = lower(a.address) and lower(info.chain) = lower(a.chain)
        -- and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(t.block_time as  TIMESTAMP)), 300) * 300)) = a.timestamp 
        """


def dex_swap_view_warp():
    return """
    select
    k.*,
    --  SAFE_MULTIPLY(k.token_a_price, k.token_a_amount) as usd_amount
    coalesce(SAFE_MULTIPLY(k.token_a_price, k.token_a_amount),SAFE_MULTIPLY(k.token_b_price, k.token_b_amount)) as usd_amount
    from (
        select
            t.block_time,
            coalesce(t.token_a_symbol,erc_a.symbol) as token_a_symbol,
            coalesce(t.token_b_symbol,erc_b.symbol) as token_b_symbol,
            coalesce(t.token_a_amount,SAFE_DIVIDE(CAST(token_a_amount_raw AS FLOAT64), POW(10, erc_a.decimals))) as token_a_amount,
            coalesce(t.token_b_amount,SAFE_DIVIDE(CAST(token_b_amount_raw AS FLOAT64), POW(10, erc_b.decimals))) as token_b_amount,
            t.project,
            t.version,
            t.category,
            t.protocol_id,
            t.trader_a,
            t.trader_b,
            t.token_a_amount_raw,
            t.token_b_amount_raw,
            t.token_a_address,
            t.token_b_address,
            t.exchange_contract_address,
            t.tx_hash,
            t.block_number,
            t.tx_from,
            t.tx_to,
            t.trace_address,
            token_a_price,
            token_b_price,
            name,
            t.chain,
            protocol_slug 
            from(
                select t0.*,
                a.price AS token_a_price,
                b.price AS token_b_price,
                info.chain as chain,
                info.slug as protocol_slug,
                info.name
                from ({dex_source}) t0
                left join `xed-project-237404.footprint_etl.defi_protocol_info` info
                on info.protocol_id = t0.protocol_id
                 left join  `footprint-etl-internal.view_to_table.fixed_price`a 
                on lower(t0.token_a_address) = lower(a.address) and lower(info.chain) = lower(a.chain)   
                and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(t0.block_time as  TIMESTAMP)), 300) * 300)) = a.timestamp 
                left join     `footprint-etl-internal.view_to_table.fixed_price`b 
                on lower(t0.token_b_address) = lower(b.address) and lower(info.chain) = lower(b.chain)
                and ( TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(t0.block_time as  TIMESTAMP)), 300) * 300) ) = b.timestamp
                )  t
            left join `xed-project-237404.footprint_etl.erc20_all`erc_a
            on lower(t.token_a_address) = lower(erc_a.contract_address) and t.chain = erc_a.chain
            left join `xed-project-237404.footprint_etl.erc20_all` erc_b
            on lower(t.token_b_address) = lower(erc_b.contract_address) and t.chain=erc_b.chain
        ) k
    """


def create_dex_add_liquidity_view(projects):
    all_table = []
    for project in projects:
        if not project.skip_liquidity:
            all_table.append(project.get_daily_table_name('add_liquidity') + '_all')
    AllDataViewBuilder.build_multiply_data_view(
        all_table,
        'footprint-etl.footprint_dex.dex_add_liquidity',
        dex_liquidity_view_warp()
    )


def create_dex_remove_liquidity_view(projects):
    all_table = []
    for project in projects:
        if not project.skip_liquidity:
            all_table.append(project.get_daily_table_name('remove_liquidity') + '_all')
    AllDataViewBuilder.build_multiply_data_view(
        all_table,
        'footprint-etl.footprint_dex.dex_remove_liquidity',
        dex_liquidity_view_warp()
    )


def validate_before_view(projects):
    sql = """SELECT distinct lower(project) as project FROM `footprint-etl.footprint_dex.dex_trades`"""
    df_data = query_bigquery(sql)
    query_project = pydash.get(df_data.to_dict(orient='list'), 'project')
    project_names = []

    for project in projects:
        project_name = project.project_name.split('_')[2].lower()
        if project_name not in query_project:
            project.validate(project.get_daily_table_name())
            project.validate(project.get_history_table_name())


def create_dex_swap_view(projects):
    all_table = []
    for project in projects:
        all_table.append(project.get_daily_table_name('swap') + '_all')
    AllDataViewBuilder.build_multiply_data_view(
        all_table,
        'footprint-etl.footprint_dex.dex_trades',
        dex_swap_view_warp()
    )


def view_creator_runner(business_type: str, projects=None):
    assert business_type in DexModel.business_type.keys(), f'business_type must in {DexModel.business_type.keys()}'
    projects = deepcopy(projects) if projects is not None else all_project
    if business_type == DexModel.business_type['swap']:
        return create_dex_swap_view(projects)
    elif business_type == DexModel.business_type['add_liquidity']:
        return create_dex_add_liquidity_view(projects)
    elif business_type == DexModel.business_type['remove_liquidity']:
        return create_dex_remove_liquidity_view(projects)
    raise Exception('business_type Error')


if __name__ == '__main__':

    # for project in all_project:
        # date_str = '2021-11-16'
        # for index in range(10):
        #     print(index)
        # business = project.get_business_type(project.business_type['swap'])
        # business.run_daily_job()
        # business.create_all_data_view()
        # business.external_validate(date_str=(datetime.strptime(date_str,'%Y-%m-%d')+timedelta(days=-7)).strftime('%Y-%m-%d'))

    # create_dex_add_liquidity_view(all_project)
    # create_dex_remove_liquidity_view(all_project)
    create_dex_swap_view(all_project)
