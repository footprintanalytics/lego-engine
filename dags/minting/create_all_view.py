from copy import deepcopy
from utils.bigquery_utils import create_or_update_view, show_dataset_list, show_tale_list


project_id = 'footprint-etl'


def get_minting_dataset_id_list():
    datalist = show_dataset_list(project_id=project_id)
    return list(filter(lambda n: 'minting_' in n, list(map(lambda i: i.dataset_id, datalist))))


def merge_all_minting_view(sql_key: str = None):
    _sql_list_json = {
        'minting_borrow': [],
        'minting_repay': [],
        'minting_supply': [],
        'minting_withdraw': [],
        'minting_liquidation': []
    }
    if sql_key is not None:
        assert sql_key in _sql_list_json.keys(), 'sql_key_error'
        sql_list_json = {sql_key: []}
    else:
        sql_list_json = deepcopy(_sql_list_json)

    ## 拼接每个平台各个业务sql
    for dataset_id in get_minting_dataset_id_list():
        for key in sql_list_json:
            sql_list_json[key] += [get_minting_sql(key=key, dataset_id=dataset_id)]

    ## 合并sql
    for key, value in sql_list_json.items():
        if key == 'minting_liquidation':
            sql_str = get_merge_all_minting_liquidation_sql(value)
        else:
            sql_str = get_merge_all_minting_sql(value)
        if sql_str:
            create_or_update_view('footprint-etl.footprint_minting.{}'.format(key), sql_str)

def get_merge_all_minting_liquidation_sql(sql_list):
    sql_list = list(filter(None, sql_list))
    if len(sql_list) == 0:
        return
    all_source_sql = ' UNION ALL '.join(sql_list)
    return '''
            SELECT 
            all_info.*,
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

def get_minting_sql(dataset_id, key):
    tables = show_tale_list(project_id, dataset_id)
    match_table = list(filter(lambda n: key in n.table_id and 'all' in n.table_id, tables))
    return '''
      SELECT * FROM `{project}.{dataset_id}.{table_id}`
    '''.format(project=match_table[0].project, dataset_id=match_table[0].dataset_id, table_id=match_table[0].table_id) if len(
        match_table) > 0 else None


def get_merge_all_minting_sql(sql_list):
    sql_list = list(filter(None, sql_list))
    if len(sql_list) == 0:
        return
    all_source_sql = ' UNION ALL '.join(sql_list)
    return '''
            SELECT 
            all_info.*,
            all_info.token_amount * p.price as usd_value,
            all_info.operator as borrower,
            d.name
            FROM (
            {}
            ) all_info
            LEFT JOIN `footprint-etl-internal.view_to_table.fixed_price` p 
            on lower(all_info.token_address) = lower(p.address) and lower(all_info.chain) = lower(p.chain)
            and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(all_info.block_time as  TIMESTAMP)), 300) * 300)) = p.timestamp      
            LEFT JOIN `xed-project-237404.footprint.defi_protocol_info` d
            on all_info.protocol_id = d.protocol_id      
            '''.format(all_source_sql)


if __name__ == '__main__':
    ### 合并所有平台的视图
    merge_all_minting_view()
