import datetime
import os
from defi360.common.contract_adapter import ContractAdapter
from utils.query_bigquery import query_bigquery
from defi360.utils.multicall import load_abi, multi_call, BlockNumber
import pydash
import moment
import pandas as pd
from config import project_config
from defi360.utils.file_cash import FileCache
from utils.common import read_file


# todo 目前仅支持izumi
class PoolHolders(ContractAdapter):
    # 自己确定数据集
    data_set = "pool_holders"
    task_name = "pool_holders"
    time_partitioning_field = "day"
    execution_time = "35 2 * * *"
    schema_name = "defi360/schema/pool_holders.json"
    abi = "defi360/contracts_adapter/abi/izumi.json"
    depend_sql_path = 'defi360/contracts_adapter/pool_holders/depend_data.sql'
    history_date = '2022-02-20'
    pool_data = [
        {
            "chain": "Ethereum",
            "pool_address": "0x8981c60ff02cdbbf2a6ac1a9f150814f9cf68f62",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Ethereum",
            "pool_address": "0xbe138ad5d41fdc392ae0b61b09421987c1966cc3",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Ethereum",
            "pool_address": "0x57aff370686043b5d21fdd76ae4b513468b9fb3c",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Ethereum",
            "pool_address": "0x99cc0a41f8006385f42aed747e2d3642a226d06e",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Polygon",
            "pool_address": "0x01cc44fc1246d17681b325926865cdb6242277a5",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Polygon",
            "pool_address": "0x28d7bff13c5a1227aee2e892f8d22d8a1a84a0d4",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Polygon",
            "pool_address": "0x150848c11040f6e52d4802bffaffbd57e6264737",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Polygon",
            "pool_address": "0xafd5f7a790041761f33bfbf3df1b54df272f2576",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }, {
            "chain": "Polygon",
            "pool_address": "0x99cc0a41f8006385f42aed747e2d3642a226d06e",
            "nft_address": "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        }
    ]

    # 各自实现合约取数逻辑
    # 1、从deposit找到所有存入用户
    # 2、通过pool的getTokenIds方法得到pool_holders数据
    def get_daily_data(self, run_date: str, block_numbers: list):
        # 找到所有链的存入用户
        deposit_user_csv = 'defi360/contracts_adapter/cache/deposit_user-{}.csv'.format(
            datetime.datetime.utcnow().strftime('%Y-%m-%d')
        )
        file_cache = FileCache()

        if file_cache.exist_cash_name(deposit_user_csv):
            deposit_user = file_cache.get_csv_data(deposit_user_csv)
        else:
            deposit_user = self.get_depend_data(run_date)
            file_cache.cash_csv_data(deposit_user, deposit_user_csv)
            deposit_user = file_cache.get_csv_data(deposit_user_csv)

        contract_data_list = []
        for pool in self.pool_data:
            chain = pool["chain"]
            pool_address = pool["pool_address"]
            nft_address = pool["nft_address"]
            print('chain: ', chain, ' pool_address: ', pool_address, 'nft_address: ', nft_address)
            block_number = pydash.get(pydash.find(block_numbers, {"chain": chain, "date": run_date}), "block")

            # 获取deposit_user数据
            deposit_user_dicts = pydash.filter_(
                deposit_user.to_dict('records'),
                lambda user: pydash.get(user, "chain") == chain and pydash.get(user, "pool_address").lower() == pool_address.lower()
            )
            deposit_user_filter = pydash.filter_(
                deposit_user_dicts,
                lambda user: datetime.datetime.strptime(pydash.get(user, "date"), '%Y-%m-%d').date() <= datetime.datetime.strptime(run_date, '%Y-%m-%d').date()
            )
            deposit_users = list(set(pydash.map_(deposit_user_filter, lambda user: pydash.get(user, 'user'))))

            # 调用合约
            contract_data = self.get_token_ids(
                chain=chain,
                pool_address=pool_address,
                wallet_address=deposit_users,
                block_number=block_number,
                nft_address=nft_address,
                day=run_date
            )
            contract_data_list.extend(contract_data)

        df = pd.DataFrame(contract_data_list)
        print(df.head())
        return df

    # 找到所有存入用户
    def get_depend_data(self, run_date):
        sql_list = []
        chains = list(set(pydash.map_(self.pool_data, lambda pool: pydash.get(pool, 'chain'))))
        print("chains: ", chains)

        dags_folder = project_config.dags_folder
        sql_path = os.path.join(dags_folder, self.depend_sql_path)
        depend_data_sql = read_file(sql_path)

        for chain in chains:
            _sql = depend_data_sql
            sql_list.append(_sql.format(chain=chain))
        try:
            sql = " union all ".join(sql_list)
            deposit_result = query_bigquery(sql)
            return deposit_result
        except Exception as e:
            print(e)

        return []

    # 获取pool holders数据
    def get_token_ids(
            self,
            chain: str = 'Ethereum',
            block_number: int = None,
            pool_address: str = None,
            nft_address: str = None,
            wallet_address: list = None,
            day: str = None
    ):
        abi = load_abi(self.abi, 'getTokenIds')
        calls = pydash.map_(wallet_address, lambda ud: {
            'target': pool_address,
            'params': ud
        })
        result = multi_call(calls, abi, block=block_number, chain=chain)
        result_list = []
        for r_dict in result:
            for r_output in pydash.get(r_dict, 'output'):
                result_list.append({
                    "day": day,
                    "chain": chain,
                    "nft_address": nft_address,
                    "pool_address": pool_address,
                    "wallet_address": pydash.get(r_dict, 'params.0'),
                    "token_id": r_output
                })
        return result_list


if __name__ == '__main__':
    poolHolders = PoolHolders()
    #
    # # 跑当天数据
    # poolHolders.load_daily_data(debug=True)

    # 跑历史数据
    # poolHolders.load_history_data(debug=True)

    # 校验
    # poolHolders.validate()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
