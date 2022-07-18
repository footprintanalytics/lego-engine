
from utils.bigquery_utils import create_or_update_view, show_dataset_list, show_tale_list
from minting.ethereum.maker import MakerMintingRepay, MakerMintingBorrow, MakerMintingWithdraw, MakerMintingSupply
from minting.ethereum.frax import FraxMintingRepay, FraxMintingBorrow, FraxMintingWithdraw, FraxMintingSupply
from minting.ethereum.liquity import LiquityMintingLiquidation, LiquityMintingWithdraw, LiquityMintingBorrow, \
    LiquityMintingRepay, LiquityMintingSupply


project_id = 'footprint-etl'


def get_minting_dataset_id_list():
    datalist = show_dataset_list(project_id=project_id)
    return list(filter(lambda n: 'minting_' in n, list(map(lambda i: i.dataset_id, datalist))))


def merge_all_minting_view():
    sql_list_json = {
        'minting_borrow': [],
        'minting_collateral_change': [],
        'minting_repay': [],
        'minting_supply': [],
        'minting_withdraw': []
    }

    ## 拼接每个平台各个业务sql
    for dataset_id in get_minting_dataset_id_list():
        for key in sql_list_json:
            sql_list_json[key] += [get_minting_sql(key=key, dataset_id=dataset_id)]

    ## 合并sql
    for key in sql_list_json:
        sql_str = get_merge_all_minting_sql(sql_list_json.get(key))
        if sql_str:
            create_or_update_view('footprint-etl.footprint_minting.{}'.format(key), sql_str)


def get_minting_sql(dataset_id, key):
    # print(dataset_id)
    # if dataset_id not in ['ethereum_minting_liquity', 'ethereum_minting_maker', 'ethereum_minting_frax']:
    #     return ''
    tables = show_tale_list(project_id, dataset_id)
    match_table = list(filter(lambda n: key in n.table_id and 'all' in n.table_id, tables))
    return '''
      SELECT * FROM `{project}.{dataset_id}.{table_id}`
    '''.format(project=match_table[0].project, dataset_id=match_table[0].dataset_id, table_id=match_table[0].table_id) if len(
        match_table) > 0 else None


def get_merge_all_minting_sql(sql_list):
    if len(list(filter(None, sql_list))) == 0:
        return
    # sql_list = filter(None, sql_list)
    all_source_sql = ' UNION ALL '.join(sql_list)
    return '''
            SELECT 
            all_info.*,
            
            -- 兼容旧字段
            all_info.block_time as block_timestamp,
            all_info.tx_hash as transaction_hash,
            all_info.token_address as asset_address,
            all_info.token_symbol as asset_symbol,

            all_info.token_amount * p.price as usd_value
            FROM (
            {}
            ) all_info
            LEFT JOIN `xed-project-237404.footprint_etl.token_daily_price` p 
            ON LOWER(all_info.token_address) = LOWER(p.address) AND p.day = Date(all_info.block_time)         
            '''.format(all_source_sql)


all_project = [
    FraxMintingWithdraw,
    FraxMintingSupply,
    FraxMintingBorrow,
    FraxMintingRepay,
    MakerMintingSupply,
    MakerMintingWithdraw,
    MakerMintingBorrow,
    MakerMintingRepay,
    LiquityMintingLiquidation,
    LiquityMintingWithdraw,
    LiquityMintingBorrow,
    LiquityMintingRepay,
    LiquityMintingSupply
]


if __name__ == '__main__':
    ### 合并所有平台的视图
    merge_all_minting_view()
