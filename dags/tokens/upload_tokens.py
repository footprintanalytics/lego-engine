import os

import pandas as pd
from google.cloud import bigquery

from config import project_config
from utils.import_gsc_to_bigquery import get_schema

big_query_client = bigquery.Client()


def load_to_bigquery(table_id, csv_file, schema):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_file, "rb") as source_file:
        job = big_query_client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    table = big_query_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def upload_all_pools_info():
    pools = [
        'yield_aggregator/curve/curve_v1_pools_info.csv',
        'yield_aggregator/convex/convex_pools_info.csv',
        'yield_aggregator/aave/aave_pools_info.csv',
        'yield_aggregator/bprotocol/bprotocol_pools_info.csv',
        'yield_aggregator/liquity/liquity_pools_info.csv',
        'yield_aggregator/pendle/pendle_pools_info.csv',
        'yield_aggregator/yearn_finance/yearn_finance_pools_info.csv',
        'yield_aggregator/maker_dao/maker_dao_pools_info.csv',
        'yield_aggregator/badger_finance/badger_pools_info.csv',
        'yield_aggregator/alchemix/alchemix_pools_info.csv',
        'yield_aggregator/trueFi/trueFi_pools_info.csv',
        'yield_aggregator/harvest/harvest_pools_info.csv',
        'yield_aggregator/cream_finance/cream_finance_pools_info.csv'
    ]
    dags_folder = project_config.dags_folder
    pools = list(map(lambda it: os.path.join(dags_folder, it), pools))
    combined_csv = pd.concat([pd.read_csv(f) for f in pools])

    file_name = 'all_pools_info'
    one_file_path = f'./{file_name}.csv'
    combined_csv.to_csv(one_file_path, index=False)
    print(f'merge {len(pools)} files to one {one_file_path}')

    table_name = 'footprint_etl.{}'.format(file_name)
    csv_path = './{}.csv'.format(file_name)
    schema = get_schema(file_name)
    print(table_name, csv_path)
    load_to_bigquery(table_name, csv_path, schema)


def upload_all():
    for file_name in [
        # 'erc20_tokens',
        'pool_infos',
        # 'polygon_erc20_tokens'
        # 'bsc_erc20_tokens'
        # 'erc20_stablecoins',
        # 'compound_view_ctokens',
        # 'makermcd_collateral_addresses',
        # 'yearn_vaults',
    ]:
        table_name = 'footprint_etl.{}'.format(file_name)
        csv_path = './{}.csv'.format(file_name)
        schema = get_schema(file_name)
        print(table_name, csv_path)
        load_to_bigquery(table_name, csv_path, schema)


if __name__ == '__main__':
    upload_all()
    # note by Pen: 如果修改了erc20_tokens 以及其他链的token信息
    # 请不要执行这个文件, 改为将这个文件放到目录
    # 然后重新运行 'foot_print_{chain}_erc20_tokens' dag的footprint_load_{chain}_erc20 task任务即可.
