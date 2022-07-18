from copy import deepcopy
from lending.ethereum.aave import AaveLendingBorrow,\
    AaveLendingRepay, AaveLendingSupply, AaveLendingWithdraw, AaveLendingLiquidation
from lending.polygon.aave import AaveLendingBorrow as PolygonAaveLendingBorrow,\
    AaveLendingWithdraw as PolygonAaveLendingWithdraw, AaveLendingSupply as PolygonAaveLendingSupply,\
    AaveLendingLiquidation as PolygonAaveLendingLiquidation, AaveLendingRepay as PolygonAaveLendingRepay
from lending.ethereum.bzx import BzxLendingRepay, BzxLendingBorrow, BzxLendingSupply, BzxLendingWithdraw
from lending.ethereum.compound import CompoundLendingBorrow,\
    CompoundLendingRepay, CompoundLendingSupply, CompoundLendingWithdraw, CompoundLendingLiquidation
from lending.ethereum.cream import CreamLendingRepay, CreamLendingBorrow, CreamLendingWithdraw,\
    CreamLendingSupply, CreamLendingLiquidation
from lending.ethereum.maple import MapleLendingWithdraw, MapleLendingSupply
from lending.ethereum.truefi import TruefiLendingRepay, TruefiLendingBorrow, TruefiLendingSupply, TruefiLendingWithdraw

from lending.bsc.dforce import DforceLendingRepay, DforceLendingBorrow, DforceLendingSupply, DforceLendingWithdraw
from lending.bsc.venus import VenusLendingRepay, VenusLendingBorrow, VenusLendingWithdraw, VenusLendingSupply,\
    VenusLendingLiquidation
from utils.bigquery_utils import create_or_update_view, show_dataset_list, show_tale_list
from lending.ethereum.raricapital import RaricapitalLendingRepay, RaricapitalLendingBorrow, RaricapitalLendingLiquidation,\
    RaricapitalLendingSupply, RaricapitalLendingWithdraw


project_id = 'footprint-etl'


def get_lending_dataset_id_list():
    datalist = show_dataset_list(project_id=project_id)
    return list(filter(lambda n: 'lending_' in n, list(map(lambda i: i.dataset_id, datalist))))


def merge_all_lending_view(sql_key: str = None):
    _sql_list_json = {
        'lending_borrow': [],
        # 'lending_collateral_change': [],
        'lending_repay': [],
        'lending_supply': [],
        'lending_withdraw': [],
        'lending_liquidation': []
    }
    if sql_key is not None:
        assert sql_key in _sql_list_json.keys(), 'sql_key_error'
        sql_list_json = {sql_key: []}
    else:
        sql_list_json = deepcopy(_sql_list_json)

    ## 拼接每个平台各个业务sql
    for dataset_id in get_lending_dataset_id_list():
        for key in sql_list_json:
            sql_list_json[key] += [get_lending_sql(key=key, dataset_id=dataset_id)]

    ## 合并sql
    for key, value in sql_list_json.items():
        if key == 'lending_liquidation':
            sql_str = get_merge_all_lending_liquidation_sql(value)
        else:
            sql_str = get_merge_all_lending_sql(value)
        if sql_str:
            create_or_update_view('footprint-etl.footprint_lending.{}'.format(key), sql_str)


def get_merge_all_lending_liquidation_sql(sql_list):
    sql_list = list(filter(None, sql_list))
    if len(sql_list) == 0:
        return
    all_source_sql = ' UNION ALL '.join(sql_list)
    return '''
            SELECT 
            all_info.*,

            -- 兼容旧字段
            all_info.block_time as block_timestamp,
            all_info.tx_hash as transaction_hash,

            all_info.token_collateral_amount * p.price as token_collateral_usd_value,
            all_info.repay_token_amount * pr.price as repay_token_usd_value,
            d.name
            FROM (
            {}
            ) all_info
            LEFT JOIN `footprint-etl-internal.view_to_table.fixed_price` p 
            on lower(all_info.token_collateral_address) = lower(p.address) and lower(all_info.chain) = lower(p.chain)   
            and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(all_info.block_time as  TIMESTAMP)), 300) * 300)) = p.timestamp 
            LEFT JOIN `footprint-etl-internal.view_to_table.fixed_price` pr 
            on lower(all_info.repay_token_address) = lower(pr.address) and lower(all_info.chain) = lower(pr.chain)   
            and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(all_info.block_time as  TIMESTAMP)), 300) * 300)) = pr.timestamp   
            LEFT JOIN `xed-project-237404.footprint.defi_protocol_info` d
            on all_info.protocol_id = d.protocol_id
            '''.format(all_source_sql)

def get_lending_sql(dataset_id, key):
    tables = show_tale_list(project_id, dataset_id)
    match_table = list(filter(lambda n: key in n.table_id and 'all' in n.table_id, tables))
    return '''
      SELECT * FROM `{project}.{dataset_id}.{table_id}`
    '''.format(project=match_table[0].project, dataset_id=match_table[0].dataset_id, table_id=match_table[0].table_id) if len(
        match_table) > 0 else None


def get_merge_all_lending_sql(sql_list):
    sql_list = list(filter(None, sql_list))
    if len(sql_list) == 0:
        return
    all_source_sql = ' UNION ALL '.join(sql_list)
    return '''
            SELECT 
            all_info.*,
            
            -- 兼容旧字段
            all_info.block_time as block_timestamp,
            all_info.tx_hash as transaction_hash,
            all_info.token_address as asset_address,
            all_info.token_symbol as asset_symbol,
            all_info.operator as borrower,
            
            all_info.token_amount * p.price as usd_value,
            d.name as name 
            FROM (
            {}
            ) all_info
            LEFT JOIN `footprint-etl-internal.view_to_table.fixed_price` p 
            on lower(all_info.token_address) = lower(p.address) and lower(all_info.chain) = lower(p.chain)
            and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(all_info.block_time as  TIMESTAMP)), 300) * 300)) = p.timestamp           
            LEFT JOIN `xed-project-237404.footprint.defi_protocol_info` d
            on all_info.protocol_id = d.protocol_id
            '''.format(all_source_sql)


all_project = [
    AaveLendingRepay,
    AaveLendingSupply,
    AaveLendingWithdraw,
    AaveLendingBorrow,
    AaveLendingLiquidation,
    # AaveLendingCollateralChange,
    PolygonAaveLendingLiquidation,
    PolygonAaveLendingWithdraw,
    PolygonAaveLendingSupply,
    PolygonAaveLendingRepay,
    PolygonAaveLendingBorrow,
    BzxLendingWithdraw,
    BzxLendingSupply,
    BzxLendingRepay,
    BzxLendingBorrow,
    CompoundLendingSupply,
    CompoundLendingWithdraw,
    CompoundLendingBorrow,
    CompoundLendingRepay,
    # CompoundLendingCollateralChange,
    CompoundLendingLiquidation,
    CreamLendingBorrow,
    CreamLendingWithdraw,
    CreamLendingRepay,
    CreamLendingSupply,
    CreamLendingLiquidation,
    # MakerLendingRepay,
    # MakerLendingBorrow,
    # MakerLendingCollateralChange,
    # MapleLendingRepay,
    # MapleLendingBorrow,
    MapleLendingSupply,
    MapleLendingWithdraw,
    TruefiLendingBorrow,
    TruefiLendingSupply,
    TruefiLendingWithdraw,
    TruefiLendingRepay,
    DforceLendingWithdraw,
    DforceLendingSupply,
    DforceLendingBorrow,
    DforceLendingRepay,
    VenusLendingWithdraw,
    VenusLendingSupply,
    VenusLendingRepay,
    VenusLendingBorrow,
    VenusLendingLiquidation,
    RaricapitalLendingRepay,
    RaricapitalLendingBorrow,
    RaricapitalLendingLiquidation,
    RaricapitalLendingSupply,
    RaricapitalLendingWithdraw
]


if __name__ == '__main__':
    # 合并所有平台的视图
    merge_all_lending_view()

    # for project in all_project:
    #     project.parse_history_data()
    #     project.run_daily_job()
