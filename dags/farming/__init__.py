from copy import deepcopy
from utils.bigquery_utils import create_or_update_view, show_dataset_list, show_tale_list


project_id = 'footprint-etl'


def get_farming_dataset_id_list():
    datalist = show_dataset_list(project_id=project_id)
    return list(filter(lambda n: 'farming_' in n, list(map(lambda i: i.dataset_id, datalist))))


def merge_all_farming_view(sql_key: str = None):
    _sql_list_json = {
        'farming_supply': [],
        'farming_withdraw': [],
        'farming_reward': [],
        'farming_borrow': [],
        'farming_repay': [],
        'farming_collateral_add': [],
        'farming_collateral_remove': [],
        'farming_position_add': [],
        'farming_position_remove': [],
    }

    if sql_key is not None:
        assert sql_key in _sql_list_json.keys(), 'sql_key_error'
        sql_list_json = {sql_key: []}
    else:
        sql_list_json = deepcopy(_sql_list_json)

    ## 拼接每个平台各个业务sql
    for dataset_id in get_farming_dataset_id_list():
        for key in sql_list_json:
            sql_list_json[key] += [get_farming_sql(key=key, dataset_id=dataset_id)]

    ## 合并sql
    for key, value in sql_list_json.items():
        sql_str = get_merge_all_farming_sql(value)
        if sql_str:
            create_or_update_view('footprint-etl.footprint_farming.{}'.format(key), sql_str)


def get_farming_sql(dataset_id, key):

    tables = show_tale_list(project_id, dataset_id)
    match_table = list(filter(lambda n: key in n.table_id and 'all' in n.table_id, tables))
    return '''
      SELECT * FROM `{project}.{dataset_id}.{table_id}`
    '''.format(project=match_table[0].project, dataset_id=match_table[0].dataset_id, table_id=match_table[0].table_id) if len(
        match_table) > 0 else None


def get_merge_all_farming_sql(sql_list):
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
            
            all_info.token_amount * p.price as usd_value
            FROM (
            {}
            ) all_info
            LEFT JOIN `footprint-etl-internal.view_to_table.fixed_price` p 
            ON LOWER(all_info.token_address) = LOWER(p.address) 
            AND  lower(all_info.chain) = lower(p.chain) 
            AND (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(all_info.block_time as  TIMESTAMP)), 300) * 300)) = p.timestamp         
            '''.format(all_source_sql)


if __name__ == '__main__':
    ### 合并所有平台的视图
    merge_all_farming_view()
